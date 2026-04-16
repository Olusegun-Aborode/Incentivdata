'use client';

import React, { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import TuiPanel from '@/components/TuiPanel';
import MetricCard from '@/components/MetricCard';
import ChartWrapper from '@/components/ChartWrapper';
import DataTable from '@/components/DataTable';
import AddressLink from '@/components/AddressLink';
import PasswordGate from '@/components/PasswordGate';
import { formatCompact, apiUrl } from '@/lib/helpers';

interface EnrichedData {
  summary: {
    totalWalletsEnriched: number;
    uniqueProtocols: number;
    chainsActive: number;
    avgPostBridgeTimeHours: string;
  };
  topProtocols: {
    name: string;
    category: string;
    tx_count: number;
    volume_usd: number;
    unique_wallets: number;
  }[];
  chainDistribution: {
    chain: string;
    chain_id: number;
    wallet_count: number;
    tx_count: number;
    volume_usd: number;
  }[];
  postBridgeActions: {
    action_type: string;
    count: number;
    avg_hours: number;
  }[];
  protocolsByChain: {
    chain: string;
    chain_id: number;
    protocols: {
      name: string;
      category: string;
      tx_count: number;
      volume_usd: number;
      unique_wallets: number;
    }[];
  }[];
  walletTable: {
    wallet: string;
    chains_active: number;
    protocols_used: number;
    total_txs: number;
    top_protocol: string;
    bridge_direction: string;
  }[];
  postBridgeFlow: {
    destination_chain: string;
    action_type: string;
    protocol: string;
    count: number;
    avg_time_hours: number;
  }[];
  lastUpdated: string | null;
}

interface WalletDetail {
  wallet: string;
  protocolsByChain: {
    chain: string;
    protocols: {
      name: string;
      category: string;
      tx_count: number;
      volume_usd: number;
      direction: string;
    }[];
  }[];
  chains: {
    chain: string;
    tx_count: number;
    volume_usd: number;
    first_seen: string;
    last_seen: string;
  }[];
  postBridgeActions: {
    next_action_type: string;
    next_action_protocol: string;
    next_action_chain: string;
    time_to_action_seconds: number;
  }[];
  hasData: boolean;
}

function ExpandableWalletRow({ wallet }: { wallet: string }) {
  const { data, isLoading } = useQuery<WalletDetail>({
    queryKey: ['wallet-detail', wallet],
    queryFn: async () => {
      const r = await fetch(apiUrl(`/api/incentiv/bridge/enriched/wallet?address=${wallet}`));
      return r.json();
    },
  });

  if (isLoading) {
    return (
      <div className="px-4 py-3 bg-background/50 border-t border-white/5">
        <div className="flex items-center gap-2 text-xs text-text-muted font-mono">
          <span className="animate-pulse">Loading wallet data...</span>
        </div>
      </div>
    );
  }

  if (!data?.hasData) {
    return (
      <div className="px-4 py-3 bg-background/50 border-t border-white/5">
        <div className="text-xs text-text-muted font-mono">
          No enriched data available for this wallet yet. Data will be populated on the next pipeline run.
        </div>
      </div>
    );
  }

  return (
    <div className="px-4 py-3 bg-background/50 border-t border-white/5 space-y-3">
      {/* Chain presence */}
      <div>
        <div className="text-[10px] uppercase tracking-wider text-accent-orange font-mono mb-2">
          Chain Activity
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
          {(data.chains || []).map((c) => (
            <div key={c.chain} className="bg-card rounded px-3 py-2 border border-white/5">
              <div className="text-xs font-semibold text-foreground">{c.chain}</div>
              <div className="text-[10px] text-text-muted">
                {formatCompact(c.tx_count)} txs
                {c.volume_usd > 0 && ` | $${formatCompact(c.volume_usd)}`}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Protocols by chain */}
      {(data.protocolsByChain || []).map((cp) => (
        <div key={cp.chain}>
          <div className="text-[10px] uppercase tracking-wider text-accent-cyan font-mono mb-1">
            {cp.chain} Protocols
          </div>
          <div className="flex flex-wrap gap-1.5">
            {cp.protocols.slice(0, 8).map((p) => (
              <span
                key={p.name}
                className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-mono bg-card border border-white/5"
              >
                <span className="text-foreground">{p.name}</span>
                <span className="text-text-muted">({formatCompact(p.tx_count)})</span>
              </span>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function EnrichedContent() {
  const [expandedWallet, setExpandedWallet] = useState<string | null>(null);

  const { data, isLoading, error } = useQuery<EnrichedData>({
    queryKey: ['enriched-bridge'],
    queryFn: async () => {
      const r = await fetch(apiUrl('/api/incentiv/bridge/enriched'));
      const json = await r.json();
      if (json.error) throw new Error(json.error);
      return json;
    },
    retry: 3,
    retryDelay: (attempt) => Math.min(1000 * 2 ** attempt, 10000),
  });

  // Chart data
  const protocolBarData = useMemo(() => {
    if (!data?.topProtocols) return [];
    return data.topProtocols.slice(0, 10).map(p => ({
      name: p.name,
      transactions: p.tx_count,
      wallets: p.unique_wallets,
    }));
  }, [data]);

  const chainPieData = useMemo(() => {
    if (!data?.chainDistribution) return [];
    return data.chainDistribution.map(c => ({
      name: c.chain,
      value: c.wallet_count,
    }));
  }, [data]);

  const actionBarData = useMemo(() => {
    if (!data?.postBridgeActions) return [];
    return data.postBridgeActions.map(a => ({
      name: a.action_type,
      count: a.count,
      avg_hours: a.avg_hours,
    }));
  }, [data]);

  if (error) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="tui-panel p-8 text-center">
          <div className="text-accent-red text-sm mb-2 font-mono">ERROR</div>
          <div className="text-text-muted text-xs font-mono">Failed to load enriched bridge data.</div>
        </div>
      </div>
    );
  }

  const isEmpty = !isLoading && data?.summary?.totalWalletsEnriched === 0;

  return (
    <div className="space-y-6">
      {/* Metrics */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <MetricCard
          label="Wallets Enriched"
          value={data ? formatCompact(data.summary.totalWalletsEnriched) : '-'}
          accent="orange"
          loading={isLoading}
        />
        <MetricCard
          label="Unique Protocols"
          value={data ? String(data.summary.uniqueProtocols) : '-'}
          accent="cyan"
          loading={isLoading}
        />
        <MetricCard
          label="Chains Active"
          value={data ? String(data.summary.chainsActive) : '-'}
          accent="green"
          loading={isLoading}
        />
        <MetricCard
          label="Avg Post-Bridge Time"
          value={data ? `${data.summary.avgPostBridgeTimeHours}h` : '-'}
          accent="purple"
          loading={isLoading}
        />
      </div>

      {isEmpty && (
        <TuiPanel title="Pipeline Status">
          <div className="text-center py-8">
            <div className="text-accent-orange text-sm font-mono mb-2">Awaiting Data</div>
            <div className="text-text-muted text-xs font-mono max-w-md mx-auto">
              The enrichment pipeline has not been run yet. Once executed, this page will display
              cross-chain protocol interactions, multi-chain wallet behavior, and post-bridge action analytics
              for all bridge users (past 180 days).
            </div>
          </div>
        </TuiPanel>
      )}

      {!isEmpty && (
        <>
          {/* Top Protocols + Chain Distribution */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <TuiPanel title="Top Protocols" tooltip="Most-used protocols by bridge users across all chains">
              <ChartWrapper
                data={protocolBarData as Record<string, unknown>[]}
                type="bar"
                xKey="name"
                yKeys={[
                  { key: 'transactions', color: '#FF6B35', name: 'Transactions' },
                  { key: 'wallets', color: '#5B7FFF', name: 'Unique Wallets' },
                ]}
                loading={isLoading}
                height={280}
              />
            </TuiPanel>

            <TuiPanel title="Chain Distribution" tooltip="How bridge wallets are distributed across chains">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <ChartWrapper
                  data={chainPieData as Record<string, unknown>[]}
                  type="pie"
                  pieDataKey="value"
                  pieNameKey="name"
                  loading={isLoading}
                  height={200}
                />
                <div className="space-y-2 flex flex-col justify-center px-2">
                  {(data?.chainDistribution || []).map((c, i) => {
                    const colors = ['#E55A2B', '#4A6CF7', '#059669', '#9333EA', '#0891B2', '#D97706', '#DC2626'];
                    const total = (data?.chainDistribution || []).reduce((s, x) => s + x.wallet_count, 0);
                    const pct = total > 0 ? ((c.wallet_count / total) * 100).toFixed(1) : '0';
                    return (
                      <div key={c.chain} className="flex items-center justify-between text-xs">
                        <span className="flex items-center gap-2">
                          <span className="w-2.5 h-2.5 rounded-full" style={{ background: colors[i % colors.length] }} />
                          <span className="text-foreground font-medium">{c.chain}</span>
                        </span>
                        <span className="flex items-center gap-3">
                          <span className="text-text-muted">{pct}%</span>
                          <span className="text-accent-orange font-semibold">{formatCompact(c.wallet_count)} wallets</span>
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>
            </TuiPanel>
          </div>

          {/* Post-Bridge Actions */}
          <TuiPanel title="Post-Bridge Actions" tooltip="What users do immediately after bridging out of Incentiv">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <ChartWrapper
                data={actionBarData as Record<string, unknown>[]}
                type="bar"
                xKey="name"
                yKeys={[
                  { key: 'count', color: '#9333EA', name: 'Count' },
                ]}
                loading={isLoading}
                height={220}
              />
              <div className="overflow-x-auto">
                <table className="w-full text-xs font-mono">
                  <thead>
                    <tr className="border-b border-white/8">
                      <th className="text-left py-2 px-3 text-text-muted uppercase tracking-wider">Action</th>
                      <th className="text-right py-2 px-3 text-text-muted uppercase tracking-wider">Count</th>
                      <th className="text-right py-2 px-3 text-text-muted uppercase tracking-wider">Avg Time</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(data?.postBridgeActions || []).map((a) => (
                      <tr key={a.action_type} className="border-b border-white/5 hover:bg-white/2">
                        <td className="py-2 px-3 text-foreground capitalize">{a.action_type}</td>
                        <td className="py-2 px-3 text-right text-accent-orange">{formatCompact(a.count)}</td>
                        <td className="py-2 px-3 text-right text-text-muted">{a.avg_hours}h</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </TuiPanel>

          {/* Post-Bridge Flow Detail */}
          {(data?.postBridgeFlow || []).length > 0 && (
            <TuiPanel title="Post-Bridge Flow Detail" tooltip="Breakdown of where bridged funds go and which protocols are used" noPadding>
              <DataTable
                columns={[
                  { key: 'destination_chain', header: 'Destination', render: (r) => {
                    const row = r as Record<string, unknown>;
                    return <span className="chain-badge">{row.destination_chain as string}</span>;
                  }},
                  { key: 'action_type', header: 'Action', render: (r) => {
                    const row = r as Record<string, unknown>;
                    return <span className="capitalize text-foreground">{row.action_type as string}</span>;
                  }},
                  { key: 'protocol', header: 'Protocol', render: (r) => {
                    const row = r as Record<string, unknown>;
                    return <span className="text-accent-cyan">{row.protocol as string}</span>;
                  }},
                  { key: 'count', header: 'Count', align: 'right', render: (r) => {
                    const row = r as Record<string, unknown>;
                    return <span className="text-accent-orange">{formatCompact(row.count as number)}</span>;
                  }},
                  { key: 'avg_time_hours', header: 'Avg Time', align: 'right', render: (r) => {
                    const row = r as Record<string, unknown>;
                    return <span className="text-text-muted">{row.avg_time_hours as number}h</span>;
                  }},
                ]}
                data={(data?.postBridgeFlow || []) as Record<string, unknown>[]}
                loading={isLoading}
                rowCount={10}
              />
            </TuiPanel>
          )}

          {/* Wallet Explorer */}
          <TuiPanel title="Bridge Wallet Explorer" tooltip="Top bridge wallets with cross-chain protocol activity. Click a row to see detailed breakdown." noPadding>
            <div className="overflow-x-auto">
              <table className="w-full text-xs font-mono">
                <thead>
                  <tr className="border-b border-white/8">
                    <th className="text-left py-2.5 px-3 text-text-muted uppercase tracking-wider">Wallet</th>
                    <th className="text-right py-2.5 px-3 text-text-muted uppercase tracking-wider">Chains</th>
                    <th className="text-right py-2.5 px-3 text-text-muted uppercase tracking-wider">Protocols</th>
                    <th className="text-right py-2.5 px-3 text-text-muted uppercase tracking-wider">Total Txs</th>
                    <th className="text-left py-2.5 px-3 text-text-muted uppercase tracking-wider">Top Protocol</th>
                    <th className="text-center py-2.5 px-3 text-text-muted uppercase tracking-wider">Dir</th>
                  </tr>
                </thead>
                <tbody>
                  {(data?.walletTable || []).map((w) => (
                    <React.Fragment key={w.wallet}>
                      <tr
                        className="border-b border-white/5 hover:bg-white/2 cursor-pointer transition-colors"
                        onClick={() => setExpandedWallet(expandedWallet === w.wallet ? null : w.wallet)}
                      >
                        <td className="py-2 px-3">
                          <span className="flex items-center gap-1.5">
                            <span className={`text-[10px] transition-transform ${expandedWallet === w.wallet ? 'rotate-90' : ''}`}>
                              &#9654;
                            </span>
                            <AddressLink address={w.wallet} />
                          </span>
                        </td>
                        <td className="py-2 px-3 text-right text-accent-cyan">{w.chains_active}</td>
                        <td className="py-2 px-3 text-right text-accent-blue">{w.protocols_used}</td>
                        <td className="py-2 px-3 text-right text-accent-orange">{formatCompact(w.total_txs)}</td>
                        <td className="py-2 px-3 text-foreground">{w.top_protocol}</td>
                        <td className="py-2 px-3 text-center">
                          <span className={w.bridge_direction === 'inbound' ? 'badge-in' : 'badge-out'}>
                            {w.bridge_direction === 'inbound' ? 'IN' : 'OUT'}
                          </span>
                        </td>
                      </tr>
                      {expandedWallet === w.wallet && (
                        <tr>
                          <td colSpan={6}>
                            <ExpandableWalletRow wallet={w.wallet} />
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  ))}
                </tbody>
              </table>
              {isLoading && (
                <div className="py-8 text-center text-text-muted text-xs font-mono animate-pulse">
                  Loading wallet data...
                </div>
              )}
            </div>
          </TuiPanel>

          {/* Last updated */}
          {data?.lastUpdated && (
            <div className="text-[10px] text-text-muted font-mono text-right">
              Last enrichment pipeline run: {new Date(data.lastUpdated).toLocaleString()}
            </div>
          )}
        </>
      )}
    </div>
  );
}

export default function EnrichedBridgePage() {
  return (
    <PasswordGate>
      <EnrichedContent />
    </PasswordGate>
  );
}
