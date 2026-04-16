import { NextResponse } from 'next/server';
import { query, CACHE_TTLS } from '@/lib/db';

export const dynamic = 'force-dynamic';

function cachedResponse(data: unknown) {
  return NextResponse.json(data, {
    headers: { 'Cache-Control': 'public, s-maxage=120, stale-while-revalidate=300' },
  });
}

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const page = parseInt(searchParams.get('page') || '1');
  const limit = 25;
  const offset = (page - 1) * limit;

  try {
    const [recentTxs, contractLeaderboard, eventDistribution] = await Promise.all([
      query(
        `SELECT
          block_number,
          block_timestamp,
          from_address,
          to_address,
          value,
          COALESCE(gas_used, gas, 0) as gas_used,
          gas_price,
          COALESCE(NULLIF(status, ''), '1') as status,
          hash
        FROM transactions
        ORDER BY block_number DESC
        LIMIT $1 OFFSET $2`,
        [limit, offset],
        `activity_txs_v2_p${page}`,
        CACHE_TTLS.RECENT
      ),

      query(
        `SELECT
          address,
          event_count as count
        FROM contracts
        ORDER BY event_count DESC
        LIMIT 20`,
        [],
        'activity_contract_leaderboard',
        CACHE_TTLS.LEADERBOARD
      ),

      query(
        `SELECT event_name as name, COUNT(*)::int as value
         FROM decoded_events
         GROUP BY event_name
         ORDER BY value DESC
         LIMIT 10`,
        [],
        'activity_event_distribution',
        CACHE_TTLS.COUNTS
      ),
    ]);

    return cachedResponse({
      recentTransactions: recentTxs,
      contractLeaderboard,
      eventDistribution,
      pagination: {
        page,
        limit,
        hasMore: recentTxs.length === limit,
      },
    });
  } catch (error) {
    console.error('Activity API error:', error);
    return NextResponse.json({ error: 'Failed to fetch activity data' }, { status: 500 });
  }
}
