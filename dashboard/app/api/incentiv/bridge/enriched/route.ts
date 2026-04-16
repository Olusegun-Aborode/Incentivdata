import { NextResponse } from 'next/server';
import { query, queryOne, CACHE_TTLS } from '@/lib/db';

export const dynamic = 'force-dynamic';

function cachedResponse(data: unknown) {
  return NextResponse.json(data, {
    headers: { 'Cache-Control': 'public, s-maxage=300, stale-while-revalidate=600' },
  });
}

export async function GET() {
  try {
    const [
      totalWallets,
      topProtocols,
      chainDistribution,
      postBridgeActions,
      protocolsByChain,
      walletTable,
      postBridgeFlow,
      lastUpdated,
    ] = await Promise.all([
      // Total enriched wallets
      queryOne<{ count: string }>(
        `SELECT COUNT(DISTINCT wallet_address)::text as count FROM bridge_wallet_protocols`,
        [],
        'enriched_total_wallets',
        CACHE_TTLS.COUNTS
      ),

      // Top protocols across all bridge wallets
      query(
        `SELECT
          protocol_name,
          COALESCE(protocol_category, 'unknown') as protocol_category,
          SUM(tx_count)::int as total_txs,
          COALESCE(SUM(volume_usd), 0)::numeric as total_volume_usd,
          COUNT(DISTINCT wallet_address)::int as unique_wallets
        FROM bridge_wallet_protocols
        GROUP BY protocol_name, protocol_category
        ORDER BY total_txs DESC
        LIMIT 20`,
        [],
        'enriched_top_protocols',
        CACHE_TTLS.LEADERBOARD
      ),

      // Chain distribution
      query(
        `SELECT
          chain_name,
          chain_id,
          COUNT(DISTINCT wallet_address)::int as wallet_count,
          SUM(tx_count)::int as total_txs,
          COALESCE(SUM(volume_usd), 0)::numeric as total_volume_usd
        FROM bridge_wallet_chains
        GROUP BY chain_name, chain_id
        ORDER BY wallet_count DESC`,
        [],
        'enriched_chain_distribution',
        CACHE_TTLS.LEADERBOARD
      ),

      // Post-bridge action breakdown
      query(
        `SELECT
          next_action_type,
          COUNT(*)::int as count,
          ROUND(AVG(time_to_next_action_seconds) / 3600.0, 1) as avg_hours
        FROM bridge_post_actions
        WHERE next_action_type IS NOT NULL
        GROUP BY next_action_type
        ORDER BY count DESC`,
        [],
        'enriched_post_actions',
        CACHE_TTLS.LEADERBOARD
      ),

      // Protocols grouped by chain
      query(
        `SELECT
          chain_name,
          chain_id,
          protocol_name,
          COALESCE(protocol_category, 'unknown') as protocol_category,
          SUM(tx_count)::int as total_txs,
          COALESCE(SUM(volume_usd), 0)::numeric as total_volume_usd,
          COUNT(DISTINCT wallet_address)::int as unique_wallets
        FROM bridge_wallet_protocols
        GROUP BY chain_name, chain_id, protocol_name, protocol_category
        ORDER BY chain_name, total_txs DESC`,
        [],
        'enriched_protocols_by_chain',
        CACHE_TTLS.LEADERBOARD
      ),

      // Wallet leaderboard table (top 100)
      query(
        `SELECT
          p.wallet_address,
          COUNT(DISTINCT c.chain_id)::int as chains_active,
          COUNT(DISTINCT p.protocol_name)::int as protocols_used,
          SUM(p.tx_count)::int as total_txs,
          (SELECT protocol_name FROM bridge_wallet_protocols
           WHERE wallet_address = p.wallet_address
           ORDER BY tx_count DESC LIMIT 1) as top_protocol,
          MAX(p.bridge_direction) as bridge_direction
        FROM bridge_wallet_protocols p
        LEFT JOIN bridge_wallet_chains c ON c.wallet_address = p.wallet_address
        GROUP BY p.wallet_address
        ORDER BY total_txs DESC
        LIMIT 100`,
        [],
        'enriched_wallet_table',
        CACHE_TTLS.LEADERBOARD
      ),

      // Post-bridge flow (for visualization)
      query(
        `SELECT
          COALESCE(next_action_chain_name, 'Unknown') as destination_chain,
          next_action_type as action_type,
          COALESCE(next_action_protocol, 'Unknown') as protocol,
          COUNT(*)::int as count,
          ROUND(AVG(time_to_next_action_seconds) / 3600.0, 1) as avg_time_hours
        FROM bridge_post_actions
        WHERE next_action_type IS NOT NULL
        GROUP BY next_action_chain_name, next_action_type, next_action_protocol
        ORDER BY count DESC
        LIMIT 30`,
        [],
        'enriched_post_bridge_flow',
        CACHE_TTLS.LEADERBOARD
      ),

      // Pipeline last run
      queryOne<{ updated_at: string }>(
        `SELECT MAX(updated_at)::text as updated_at FROM bridge_wallet_protocols`,
        [],
        'enriched_last_updated',
        CACHE_TTLS.COUNTS
      ),
    ]);

    // Group protocols by chain for structured response
    const chainProtocolMap = new Map<string, { chain: string; chain_id: number; protocols: unknown[] }>();
    for (const row of protocolsByChain) {
      const chainName = row.chain_name as string;
      if (!chainProtocolMap.has(chainName)) {
        chainProtocolMap.set(chainName, {
          chain: chainName,
          chain_id: row.chain_id as number,
          protocols: [],
        });
      }
      chainProtocolMap.get(chainName)!.protocols.push({
        name: row.protocol_name,
        category: row.protocol_category,
        tx_count: row.total_txs,
        volume_usd: parseFloat(String(row.total_volume_usd || 0)),
        unique_wallets: row.unique_wallets,
      });
    }

    // Calculate totals for metrics
    const totalProtocols = new Set((topProtocols || []).map(r => r.protocol_name)).size;
    const totalChains = (chainDistribution || []).length;
    const avgPostBridgeHours = (postBridgeActions || []).length > 0
      ? (postBridgeActions.reduce((sum, r) => sum + parseFloat(String(r.avg_hours || 0)), 0) / postBridgeActions.length).toFixed(1)
      : '-';

    return cachedResponse({
      summary: {
        totalWalletsEnriched: parseInt(totalWallets?.count || '0'),
        uniqueProtocols: totalProtocols,
        chainsActive: totalChains,
        avgPostBridgeTimeHours: avgPostBridgeHours,
      },
      topProtocols: (topProtocols || []).map(r => ({
        name: r.protocol_name,
        category: r.protocol_category,
        tx_count: r.total_txs,
        volume_usd: parseFloat(String(r.total_volume_usd || 0)),
        unique_wallets: r.unique_wallets,
      })),
      chainDistribution: (chainDistribution || []).map(r => ({
        chain: r.chain_name,
        chain_id: r.chain_id,
        wallet_count: r.wallet_count,
        tx_count: r.total_txs,
        volume_usd: parseFloat(String(r.total_volume_usd || 0)),
      })),
      postBridgeActions: (postBridgeActions || []).map(r => ({
        action_type: r.next_action_type,
        count: r.count,
        avg_hours: parseFloat(String(r.avg_hours || 0)),
      })),
      protocolsByChain: Array.from(chainProtocolMap.values()),
      walletTable: (walletTable || []).map(r => ({
        wallet: r.wallet_address,
        chains_active: r.chains_active,
        protocols_used: r.protocols_used,
        total_txs: r.total_txs,
        top_protocol: r.top_protocol || '-',
        bridge_direction: r.bridge_direction || '-',
      })),
      postBridgeFlow: (postBridgeFlow || []).map(r => ({
        destination_chain: r.destination_chain,
        action_type: r.action_type,
        protocol: r.protocol,
        count: r.count,
        avg_time_hours: parseFloat(String(r.avg_time_hours || 0)),
      })),
      lastUpdated: lastUpdated?.updated_at || null,
    });
  } catch (error) {
    console.error('Enriched Bridge API error:', error);
    return NextResponse.json({ error: 'Failed to fetch enriched bridge data' }, { status: 500 });
  }
}
