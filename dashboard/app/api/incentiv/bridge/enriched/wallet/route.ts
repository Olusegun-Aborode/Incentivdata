import { NextResponse } from 'next/server';
import { query, CACHE_TTLS } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const address = searchParams.get('address')?.toLowerCase();

  if (!address) {
    return NextResponse.json({ error: 'Missing address parameter' }, { status: 400 });
  }

  try {
    const [protocols, chains, postActions] = await Promise.all([
      // Protocol interactions for this wallet
      query(
        `SELECT
          chain_name,
          chain_id,
          protocol_name,
          COALESCE(protocol_category, 'unknown') as protocol_category,
          tx_count,
          COALESCE(volume_usd, 0)::numeric as volume_usd,
          last_active,
          bridge_direction
        FROM bridge_wallet_protocols
        WHERE wallet_address = $1
        ORDER BY tx_count DESC`,
        [address],
        `wallet_protocols_${address}`,
        CACHE_TTLS.LEADERBOARD
      ),

      // Chain presence for this wallet
      query(
        `SELECT
          chain_name,
          chain_id,
          tx_count,
          COALESCE(volume_usd, 0)::numeric as volume_usd,
          first_seen,
          last_seen
        FROM bridge_wallet_chains
        WHERE wallet_address = $1
        ORDER BY tx_count DESC`,
        [address],
        `wallet_chains_${address}`,
        CACHE_TTLS.LEADERBOARD
      ),

      // Post-bridge actions for this wallet
      query(
        `SELECT
          bridge_out_tx_hash,
          bridge_out_timestamp,
          bridge_out_chain_name,
          next_action_tx_hash,
          next_action_type,
          next_action_protocol,
          next_action_chain_name,
          next_action_timestamp,
          time_to_next_action_seconds,
          COALESCE(amount_usd, 0)::numeric as amount_usd
        FROM bridge_post_actions
        WHERE wallet_address = $1
        ORDER BY bridge_out_timestamp DESC
        LIMIT 20`,
        [address],
        `wallet_post_actions_${address}`,
        CACHE_TTLS.LEADERBOARD
      ),
    ]);

    // Group protocols by chain
    const chainProtocols = new Map<string, { chain: string; protocols: unknown[] }>();
    for (const row of protocols) {
      const chain = row.chain_name as string;
      if (!chainProtocols.has(chain)) {
        chainProtocols.set(chain, { chain, protocols: [] });
      }
      chainProtocols.get(chain)!.protocols.push({
        name: row.protocol_name,
        category: row.protocol_category,
        tx_count: row.tx_count,
        volume_usd: parseFloat(String(row.volume_usd || 0)),
        last_active: row.last_active,
        direction: row.bridge_direction,
      });
    }

    return NextResponse.json({
      wallet: address,
      protocolsByChain: Array.from(chainProtocols.values()),
      chains: (chains || []).map(r => ({
        chain: r.chain_name,
        chain_id: r.chain_id,
        tx_count: r.tx_count,
        volume_usd: parseFloat(String(r.volume_usd || 0)),
        first_seen: r.first_seen,
        last_seen: r.last_seen,
      })),
      postBridgeActions: (postActions || []).map(r => ({
        bridge_out_tx: r.bridge_out_tx_hash,
        bridge_out_time: r.bridge_out_timestamp,
        bridge_out_chain: r.bridge_out_chain_name,
        next_action_tx: r.next_action_tx_hash,
        next_action_type: r.next_action_type,
        next_action_protocol: r.next_action_protocol,
        next_action_chain: r.next_action_chain_name,
        next_action_time: r.next_action_timestamp,
        time_to_action_seconds: r.time_to_next_action_seconds,
        amount_usd: parseFloat(String(r.amount_usd || 0)),
      })),
      hasData: protocols.length > 0 || chains.length > 0,
    });
  } catch (error) {
    console.error('Wallet detail API error:', error);
    return NextResponse.json({ error: 'Failed to fetch wallet data' }, { status: 500 });
  }
}
