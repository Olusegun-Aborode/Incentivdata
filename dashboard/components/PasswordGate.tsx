'use client';

import { useState, useEffect } from 'react';
import { apiUrl } from '@/lib/helpers';

const STORAGE_KEY = 'datumlabs_enriched_bridge_unlocked';

export default function PasswordGate({ children }: { children: React.ReactNode }) {
  const [unlocked, setUnlocked] = useState(false);
  const [mounted, setMounted] = useState(false);
  const [password, setPassword] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setMounted(true);
    if (localStorage.getItem(STORAGE_KEY) === 'true') {
      setUnlocked(true);
    }
  }, []);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);

    if (!password.trim()) {
      setError('Please enter the access password.');
      return;
    }

    setSubmitting(true);
    try {
      const res = await fetch(apiUrl('/api/incentiv/bridge/enriched/auth'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password }),
      });

      const body = await res.json().catch(() => null);

      if (!res.ok || !body?.valid) {
        throw new Error('Invalid password. Please try again.');
      }

      localStorage.setItem(STORAGE_KEY, 'true');
      setUnlocked(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Authentication failed.');
    } finally {
      setSubmitting(false);
    }
  }

  if (!mounted) return null;
  if (unlocked) return <>{children}</>;

  return (
    <div className="relative">
      {/* Blurred content preview */}
      <div
        className="select-none max-h-[500px] overflow-hidden"
        style={{ filter: 'blur(8px)', pointerEvents: 'none' }}
      >
        {children}
      </div>

      {/* Password gate overlay */}
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-[#0B0D0F]/80 backdrop-blur-sm">
        <div className="bg-card rounded-lg border border-white/8 max-w-md w-full mx-4 shadow-2xl">
          {/* Header */}
          <div className="flex items-center justify-between px-6 py-4 border-b border-white/8">
            <div className="flex items-center gap-2">
              <img
                src={apiUrl('/branding/icon.png')}
                alt="Datum Labs"
                className="w-5 h-5"
                onError={(e) => { (e.target as HTMLImageElement).style.display = 'none'; }}
              />
              <span className="text-sm font-bold text-foreground tracking-wide uppercase font-mono">
                Enriched Bridge
              </span>
            </div>
            <span className="text-[10px] font-semibold text-accent-orange bg-accent-orange/10 px-2 py-0.5 rounded uppercase tracking-wider">
              Restricted
            </span>
          </div>

          {/* Body */}
          <div className="p-6 space-y-5">
            <div className="text-sm text-text-muted space-y-1.5 font-mono">
              <p>
                <span className="text-accent-orange font-semibold">&gt;</span> This section contains enriched analytics:
              </p>
              <p className="pl-4">Cross-chain protocol interactions</p>
              <p className="pl-4">Multi-chain wallet behavior</p>
              <p className="pl-4">Post-bridge action tracking</p>
              <p className="pl-4">Protocol volume analysis</p>
              <p className="mt-3">
                <span className="text-accent-orange font-semibold">&gt;</span> Enter password to access
              </p>
            </div>

            <form onSubmit={handleSubmit} className="space-y-3">
              <div className="flex items-center gap-2 rounded-lg px-3 py-2.5 bg-background border border-white/8 focus-within:border-accent-orange transition-colors">
                <span className="text-xs text-accent-orange font-mono">&gt;</span>
                <input
                  type="password"
                  placeholder="Access password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="flex-1 bg-transparent text-sm text-foreground placeholder:text-text-muted focus:outline-none font-mono"
                  autoFocus
                />
              </div>

              <button
                type="submit"
                disabled={submitting}
                className="w-full font-bold rounded-lg py-2.5 text-xs uppercase tracking-wider transition-colors bg-accent-orange text-white hover:bg-accent-orange/90 disabled:opacity-50 disabled:cursor-not-allowed font-mono"
              >
                {submitting ? 'Verifying...' : 'Unlock Enriched Analytics'}
              </button>
            </form>

            {error && (
              <p className="text-xs text-accent-red font-mono">
                <span className="font-semibold">[ERR]</span> {error}
              </p>
            )}

            <p className="text-[10px] text-center text-text-muted font-mono">
              Contact Datum Labs for access credentials
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
