'use client';

import React, { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import TuiPanel from '@/components/TuiPanel';
import MetricCard from '@/components/MetricCard';
import ChartWrapper from '@/components/ChartWrapper';
import DataTable from '@/components/DataTable';
import AddressLink from '@/components/AddressLink';
import TxLink from '@/components/TxLink';
import { formatRelativeTime, formatCompact, formatNumber, apiUrl } from '@/lib/helpers';
import { CHAIN_NAMES, TOKEN_DECIMALS } from '@/lib/contracts';

interface ChainFlow {
  chain: string;
  chain_id: string;
  count: number;
  unique_users: number;
}

interface BridgeData {
  metrics: {
    inbound: string;
    outbound: string;
  };
  inboundVolume: { contract_address: string; total_volume: string; tx_count: number }[];
  dailyBridge: { date: string; inbound: number; outbound: number }[];
  bridgeByToken: { contract_address: string; event_name: string; count: number }[];
  chainFlows: {
    inboundFrom: ChainFlow[];  // Chains sending assets TO Incentiv
    outboundTo: ChainFlow[];   // Chains receiving assets FROM Incentiv
  };
  recentBridge: {
    event_name: string;
    contract_address: string;
    amount: string;
    chain_id: string;
    recipient: string;
    timestamp: string;
    transaction_hash: string;
  }[];
}

function getChainName(chainId: string): string {
  const id = parseInt(chainId);
  return CHAIN_NAMES[id] || `Chain ${chainId}`;
}

function formatBridgeAmount(amount: string, contractAddr: string): string {
  if (!amount || amount === '0') return '0';
  try {
    const decimals = TOKEN_DECIMALS[contractAddr?.toLowerCase()] || 18;
    const n = parseFloat(amount) / Math.pow(10, decimals);
    return formatCompact(n);
  } catch {
    return '0';
  }
}

export default function BridgePage() {
  const { data, isLoading, error } = useQuery<BridgeData>({
    queryKey: ['bridge'],
    queryFn: async () => {
      const r = await fetch(apiUrl('/api/incentiv/bridge'));
      const json = await r.json();
      if (json.error) throw new Error(json.error);
      return json;
    },
    retry: 3,
    retryDelay: (attempt) => Math.min(1000 * 2 ** attempt, 10000),
  });

  // Prepare pie chart data for chain flows (must be before early returns)
  const inboundPieData = useMemo(() => {
    if (!data?.chainFlows?.inboundFrom) return [];
    return data.chainFlows.inboundFrom.map((c) => ({
      name: c.chain,
      value: c.count,
    }));
  }, [data]);

  const outboundPieData = useMemo(() => {
    if (!data?.chainFlows?.outboundTo) return [];
    return data.chainFlows.outboundTo.map((c) => ({
      name: c.chain,
      value: c.count,
    }));
  }, [data]);

  // Total bridge-in transfer count — single aggregate number
  const bridgeInTotalTransfers = useMemo(() => {
    if (!data?.inboundVolume || data.inboundVolume.length === 0) return '-';
    const total = data.inboundVolume.reduce((sum, v) => sum + (v.tx_count || 0), 0);
    return formatCompact(total);
  }, [data]);

  if (error) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="tui-panel p-8 text-center">
          <div className="text-accent-red text-sm mb-2">ERROR</div>
          <div className="text-text-muted text-xs">Failed to load bridge data.</div>
        </div>
      </div>
    );
  }

  const totalBridge = parseInt(data?.metrics.inbound || '0') + parseInt(data?.metrics.outbound || '0');

  return (
    <div className="space-y-6">
      {/* Metrics */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <MetricCard
          label="Total Bridge Transfers"
          value={formatCompact(totalBridge)}
          accent="orange"
          loading={isLoading}
        />
        <MetricCard
          label="Inbound (Received)"
          value={data ? formatCompact(data.metrics.inbound) : '-'}
          accent="green"
          loading={isLoading}
        />
        <MetricCard
          label="Outbound (Sent)"
          value={data ? formatCompact(data.metrics.outbound) : '-'}
          accent="purple"
          loading={isLoading}
        />
        <MetricCard
          label="Bridge In Transfers"
          value={bridgeInTotalTransfers}
          accent="cyan"
          loading={isLoading}
        />
      </div>

      {/* Bridge Volume Chart */}
      <TuiPanel title="Bridge Activity" tooltip="Daily Hyperlane bridge transfers — inbound (received from other chains) vs outbound (sent to other chains).">
        <ChartWrapper
          data={data?.dailyBridge || []}
          type="area"
          yKeys={[
            { key: 'inbound', color: '#059669', name: 'Inbound' },
            { key: 'outbound', color: '#9333EA', name: 'Outbound' },
          ]}
          gradientId="bridge-vol"
          loading={isLoading}
          height={280}
        />
      </TuiPanel>

      {/* Chain Flow Direction — Pie Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <TuiPanel title="Inbound: Chains → Incentiv" tooltip="Which chains are sending assets TO Incentiv via Hyperlane bridge. Shows transfer count and unique users per source chain.">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <ChartWrapper
              data={inboundPieData as Record<string, unknown>[]}
              type="pie"
              pieDataKey="value"
              pieNameKey="name"
              loading={isLoading}
              height={200}
            />
            <div className="space-y-2 flex flex-col justify-center px-2">
              {(data?.chainFlows?.inboundFrom || []).map((c, i) => {
                const colors = ['#E55A2B', '#4A6CF7', '#059669', '#9333EA', '#0891B2', '#D97706', '#DC2626'];
                const total = (data?.chainFlows?.inboundFrom || []).reduce((s, x) => s + x.count, 0);
                const pct = total > 0 ? ((c.count / total) * 100).toFixed(1) : '0';
                return (
                  <div key={c.chain_id} className="flex items-center justify-between text-xs">
                    <span className="flex items-center gap-2">
                      <span className="w-2.5 h-2.5 rounded-full" style={{ background: colors[i % colors.length] }} />
                      <span className="text-foreground font-medium">{c.chain}</span>
                    </span>
                    <span className="flex items-center gap-3">
                      <span className="text-text-muted">{pct}%</span>
                      <span className="text-accent-orange font-semibold">{formatCompact(c.count)}</span>
                      <span className="text-accent-cyan">{formatCompact(c.unique_users)} users</span>
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        </TuiPanel>

        <TuiPanel title="Outbound: Incentiv → Chains" tooltip="Which chains are receiving assets FROM Incentiv via Hyperlane bridge. Shows transfer count and unique users per destination.">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <ChartWrapper
              data={outboundPieData as Record<string, unknown>[]}
              type="pie"
              pieDataKey="value"
              pieNameKey="name"
              loading={isLoading}
              height={200}
            />
            <div className="space-y-2 flex flex-col justify-center px-2">
              {(data?.chainFlows?.outboundTo || []).map((c, i) => {
                const colors = ['#E55A2B', '#4A6CF7', '#059669', '#9333EA', '#0891B2', '#D97706', '#DC2626'];
                const total = (data?.chainFlows?.outboundTo || []).reduce((s, x) => s + x.count, 0);
                const pct = total > 0 ? ((c.count / total) * 100).toFixed(1) : '0';
                return (
                  <div key={c.chain_id} className="flex items-center justify-between text-xs">
                    <span className="flex items-center gap-2">
                      <span className="w-2.5 h-2.5 rounded-full" style={{ background: colors[i % colors.length] }} />
                      <span className="text-foreground font-medium">{c.chain}</span>
                    </span>
                    <span className="flex items-center gap-3">
                      <span className="text-text-muted">{pct}%</span>
                      <span className="text-accent-orange font-semibold">{formatCompact(c.count)}</span>
                      <span className="text-accent-cyan">{formatCompact(c.unique_users)} users</span>
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        </TuiPanel>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Bridge by Token */}
        <TuiPanel title="Bridge Activity by Token" noPadding>
          <DataTable
            columns={[
              { key: 'contract_address', header: 'Token', render: (r) => {
                const row = r as Record<string, unknown>;
                return <AddressLink address={row.contract_address as string} />;
              }},
              { key: 'event_name', header: 'Direction', render: (r) => {
                const row = r as Record<string, unknown>;
                const isInbound = row.event_name === 'ReceivedTransferRemote';
                return (
                  <span className={isInbound ? 'badge-in' : 'badge-out'}>
                    {isInbound ? 'IN' : 'OUT'}
                  </span>
                );
              }},
              { key: 'count', header: 'Count', align: 'right', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span className="text-accent-orange">{formatCompact(row.count as number)}</span>;
              }},
            ]}
            data={(data?.bridgeByToken || []) as Record<string, unknown>[]}
            loading={isLoading}
            rowCount={8}
          />
        </TuiPanel>

        {/* Recent Bridge Transfers */}
        <TuiPanel title="Recent Bridge Transfers" noPadding>
          <DataTable
            columns={[
              { key: 'event_name', header: 'Dir', render: (r) => {
                const row = r as Record<string, unknown>;
                const isInbound = row.event_name === 'ReceivedTransferRemote';
                return (
                  <span className={isInbound ? 'badge-in' : 'badge-out'}>
                    {isInbound ? 'IN' : 'OUT'}
                  </span>
                );
              }},
              { key: 'contract_address', header: 'Token', render: (r) => {
                const row = r as Record<string, unknown>;
                return <AddressLink address={row.contract_address as string} />;
              }},
              { key: 'amount', header: 'Amount', align: 'right', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span>{formatBridgeAmount(row.amount as string, row.contract_address as string)}</span>;
              }},
              { key: 'chain_id', header: 'Chain', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span className="chain-badge">{getChainName(row.chain_id as string)}</span>;
              }},
              { key: 'timestamp', header: 'Time', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span className="text-text-muted text-xs">{formatRelativeTime(row.timestamp as string)}</span>;
              }},
              { key: 'transaction_hash', header: 'Tx', render: (r) => {
                const row = r as Record<string, unknown>;
                return <TxLink hash={row.transaction_hash as string} />;
              }},
            ]}
            data={(data?.recentBridge || []) as Record<string, unknown>[]}
            loading={isLoading}
            rowCount={10}
          />
        </TuiPanel>
      </div>
    </div>
  );
}
