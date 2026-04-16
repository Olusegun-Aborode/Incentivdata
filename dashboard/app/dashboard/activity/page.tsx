'use client';

import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import TuiPanel from '@/components/TuiPanel';
import DataTable from '@/components/DataTable';
import ChartWrapper from '@/components/ChartWrapper';
import AddressLink from '@/components/AddressLink';
import TxLink from '@/components/TxLink';
import { formatRelativeTime, formatNumber, formatCompact, formatEther, formatGwei, apiUrl } from '@/lib/helpers';
import { getExplorerBlockUrl } from '@/lib/contracts';

interface ActivityData {
  recentTransactions: {
    block_number: number;
    block_timestamp: string;
    from_address: string;
    to_address: string;
    value: string;
    gas_used: string;
    gas_price: string;
    status: number;
    hash: string;
  }[];
  contractLeaderboard: { address: string; count: number }[];
  eventDistribution: { name: string; value: number }[];
  pagination: { page: number; limit: number; hasMore: boolean };
}

export default function ActivityPage() {
  const [page, setPage] = useState(1);

  const { data, isLoading, error } = useQuery<ActivityData>({
    queryKey: ['activity', page],
    queryFn: async () => {
      const r = await fetch(apiUrl(`/api/incentiv/activity?page=${page}`));
      const json = await r.json();
      if (json.error) throw new Error(json.error);
      return json;
    },
    retry: 3,
    retryDelay: (attempt) => Math.min(1000 * 2 ** attempt, 10000),
  });

  if (error) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="tui-panel p-8 text-center">
          <div className="text-accent-red text-sm mb-2">ERROR</div>
          <div className="text-text-muted text-xs">Failed to load activity data.</div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Recent Transactions */}
      <TuiPanel
        title="Recent Transactions"
        tooltip="Latest transactions on the Incentiv chain with block, sender, receiver, value, gas, and status details."
        noPadding
        rightContent={
          <div className="flex items-center gap-2">
            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={page === 1}
              className="time-pill disabled:opacity-30"
            >
              Prev
            </button>
            <span className="text-text-muted text-[10px]">Page {page}</span>
            <button
              onClick={() => setPage((p) => p + 1)}
              disabled={!data?.pagination.hasMore}
              className="time-pill disabled:opacity-30"
            >
              Next
            </button>
          </div>
        }
      >
        <DataTable
          columns={[
            { key: 'block_number', header: 'Block', render: (r) => {
              const row = r as Record<string, unknown>;
              return (
                <a
                  href={getExplorerBlockUrl(row.block_number as number)}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="explorer-link text-xs"
                >
                  {formatNumber(row.block_number as number)}
                </a>
              );
            }},
            { key: 'block_timestamp', header: 'Time', render: (r) => {
              const row = r as Record<string, unknown>;
              return <span className="text-text-muted text-xs">{formatRelativeTime(row.block_timestamp as string)}</span>;
            }},
            { key: 'from_address', header: 'From', render: (r) => {
              const row = r as Record<string, unknown>;
              return <AddressLink address={row.from_address as string} />;
            }},
            { key: 'to_address', header: 'To', render: (r) => {
              const row = r as Record<string, unknown>;
              return <AddressLink address={row.to_address as string} />;
            }},
            { key: 'value', header: 'Value', align: 'right', render: (r) => {
              const row = r as Record<string, unknown>;
              const val = parseFloat(row.value as string || '0');
              return <span className="text-xs">{val > 0 ? formatEther(val) : '-'}</span>;
            }},
            { key: 'gas_used', header: 'Gas', align: 'right', render: (r) => {
              const row = r as Record<string, unknown>;
              const gas = row.gas_used;
              // Show '–' if gas_used is null/0 (backfill pending) to avoid showing misleading gas limit
              return <span className="text-text-muted text-xs">{gas && Number(gas) > 0 ? formatNumber(gas as string) : '–'}</span>;
            }},
            { key: 'status', header: 'Status', render: (r) => {
              const row = r as Record<string, unknown>;
              const s = String(row.status ?? '1').toLowerCase();
              const isFail = s === 'error' || s === 'fail' || s === '0';
              return <span className={isFail ? 'badge-fail' : 'badge-success'}>{isFail ? 'FAIL' : 'OK'}</span>;
            }},
            { key: 'hash', header: 'Tx Hash', render: (r) => {
              const row = r as Record<string, unknown>;
              return <TxLink hash={row.hash as string} />;
            }},
          ]}
          data={(data?.recentTransactions || []) as Record<string, unknown>[]}
          loading={isLoading}
          rowCount={25}
        />
      </TuiPanel>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Contract Leaderboard */}
        <TuiPanel title="Contract Activity Leaderboard" tooltip="Most active smart contracts ranked by total decoded events. Includes DEX pools, bridge contracts, and AA EntryPoints." noPadding>
          <DataTable
            columns={[
              { key: 'rank', header: '#', render: (_r, i) => {
                return <span className="text-text-muted text-xs">{i + 1}</span>;
              }},
              { key: 'address', header: 'Contract', render: (r) => {
                const row = r as Record<string, unknown>;
                return <AddressLink address={row.address as string} />;
              }},
              { key: 'count', header: 'Events', align: 'right', render: (r) => {
                const row = r as Record<string, unknown>;
                return <span className="text-accent-orange">{formatCompact(row.count as number)}</span>;
              }},
            ]}
            data={(data?.contractLeaderboard || []) as Record<string, unknown>[]}
            loading={isLoading}
            rowCount={10}
          />
        </TuiPanel>

        {/* Event Distribution */}
        <TuiPanel title="Event Type Distribution" tooltip="Breakdown of decoded event types across all contracts. Shows the mix of swaps, bridges, transfers, and AA operations.">
          <ChartWrapper
            data={(data?.eventDistribution || []) as Record<string, unknown>[]}
            type="pie"
            loading={isLoading}
            height={300}
          />
          {data?.eventDistribution && (
            <div className="mt-4 grid grid-cols-2 gap-2">
              {data.eventDistribution.slice(0, 8).map((evt, i) => {
                const colors = ['#E55A2B', '#4A6CF7', '#059669', '#9333EA', '#0891B2', '#D97706', '#DC2626', '#9CA3AF'];
                return (
                  <div key={evt.name} className="flex items-center gap-2 text-xs">
                    <span className="w-2 h-2 rounded-full flex-shrink-0" style={{ background: colors[i % colors.length] }} />
                    <span className="text-text-muted truncate">{evt.name}</span>
                    <span className="text-foreground ml-auto">{formatCompact(evt.value)}</span>
                  </div>
                );
              })}
            </div>
          )}
        </TuiPanel>
      </div>
    </div>
  );
}
