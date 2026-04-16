'use client';

import { usePathname } from 'next/navigation';
import Link from 'next/link';

export default function BridgeLayout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();

  const tabs = [
    { href: '/dashboard/bridge/overview', label: 'Overview' },
    { href: '/dashboard/bridge/enriched', label: 'Enriched', locked: true },
  ];

  return (
    <div className="space-y-4">
      {/* Sub-tab navigation */}
      <div className="flex items-center gap-1 border-b border-white/8 pb-0">
        {tabs.map((tab) => {
          const isActive = pathname === tab.href ||
            (tab.href === '/dashboard/bridge/overview' && pathname === '/dashboard/bridge');
          return (
            <Link
              key={tab.href}
              href={tab.href}
              className={`
                relative px-4 py-2.5 text-xs font-mono uppercase tracking-wider transition-colors
                ${isActive
                  ? 'text-accent-orange border-b-2 border-accent-orange -mb-[1px]'
                  : 'text-text-muted hover:text-foreground border-b-2 border-transparent -mb-[1px]'
                }
              `}
            >
              <span className="flex items-center gap-1.5">
                {tab.label}
                {tab.locked && (
                  <svg className="w-3 h-3 opacity-60" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <rect x="3" y="11" width="18" height="11" rx="2" ry="2" />
                    <path d="M7 11V7a5 5 0 0 1 10 0v4" />
                  </svg>
                )}
              </span>
            </Link>
          );
        })}
      </div>

      {/* Page content */}
      {children}
    </div>
  );
}
