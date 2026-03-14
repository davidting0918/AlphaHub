#!/usr/bin/env python3
"""
EMA Cross RSI Strategy Optimizer - Minimal Fast Version
"""

import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List
from dataclasses import dataclass, field
from itertools import product
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

SLIPPAGE = 0.0005
FEE = 0.0006
POS_SIZE = 0.10
CAPITAL = 10000.0


@dataclass
class Result:
    params: Dict
    trades: List = field(default_factory=list)
    equity: List = field(default_factory=list)
    
    @property
    def pnl_pct(self): return ((self.equity[-1] - CAPITAL) / CAPITAL * 100) if self.equity else 0
    @property
    def win_rate(self): return (sum(1 for t in self.trades if t['pnl'] > 0) / len(self.trades) * 100) if self.trades else 0
    @property
    def profit_factor(self):
        gain = sum(t['pnl'] for t in self.trades if t['pnl'] > 0)
        loss = abs(sum(t['pnl'] for t in self.trades if t['pnl'] < 0))
        return gain / loss if loss > 0 else (99 if gain > 0 else 0)
    @property
    def max_dd(self):
        if not self.equity: return 0
        peak = self.equity[0]
        dd = 0
        for e in self.equity:
            peak = max(peak, e)
            dd = max(dd, (peak - e) / peak)
        return dd * 100


def ema(data, period):
    result = np.full(len(data), np.nan)
    if len(data) < period: return result
    mult = 2 / (period + 1)
    result[period-1] = np.mean(data[:period])
    for i in range(period, len(data)):
        result[i] = (data[i] - result[i-1]) * mult + result[i-1]
    return result


def rsi(data, period=14):
    result = np.full(len(data), np.nan)
    if len(data) < period + 1: return result
    deltas = np.diff(data)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    ag = np.mean(gains[:period])
    al = np.mean(losses[:period])
    result[period] = 100 - 100/(1 + ag/al) if al > 0 else 100
    for i in range(period, len(deltas)):
        ag = (ag * (period-1) + gains[i]) / period
        al = (al * (period-1) + losses[i]) / period
        result[i+1] = 100 - 100/(1 + ag/al) if al > 0 else 100
    return result


def atr(h, l, c, period=14):
    result = np.full(len(h), np.nan)
    if len(h) < period + 1: return result
    tr = [h[0] - l[0]]
    for i in range(1, len(h)):
        tr.append(max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])))
    tr = np.array(tr)
    result[period-1] = np.mean(tr[:period])
    for i in range(period, len(tr)):
        result[i] = (result[i-1] * (period-1) + tr[i]) / period
    return result


def load_data():
    data_dir = Path(__file__).parent / "data"
    symbols = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'ADA', 'AVAX', 'LINK']
    all_data = {}
    for sym in symbols:
        f = data_dir / f"{sym}_USDT_SWAP_15m.parquet"
        if f.exists():
            df = pd.read_parquet(f)
            all_data[sym] = {
                'h': df['high'].values.astype(float),
                'l': df['low'].values.astype(float),
                'c': df['close'].values.astype(float),
                'v': df['volume'].values.astype(float)
            }
    return all_data


def backtest(params, all_data):
    result = Result(params=params)
    equity = CAPITAL
    result.equity.append(equity)
    
    for sym, d in all_data.items():
        c, h, l, v = d['c'], d['h'], d['l'], d['v']
        if len(c) < 150: continue
        
        fast = ema(c, params['fast_ema'])
        slow = ema(c, params['slow_ema'])
        rsi_vals = rsi(c, 14)
        atr_vals = atr(h, l, c, 14)
        trend = ema(c, params['trend_ema']) if params['trend_ema'] > 0 else None
        
        pos = None
        for i in range(100, len(c)):
            if np.isnan(fast[i]) or np.isnan(slow[i]) or np.isnan(atr_vals[i]) or atr_vals[i] == 0:
                continue
            
            # Exit
            if pos:
                exit_p = None
                if pos['side'] == 'long':
                    if l[i] <= pos['sl']: exit_p = pos['sl']
                    elif h[i] >= pos['tp']: exit_p = pos['tp']
                    elif fast[i-1] >= slow[i-1] and fast[i] < slow[i]: exit_p = c[i]
                else:
                    if h[i] >= pos['sl']: exit_p = pos['sl']
                    elif l[i] <= pos['tp']: exit_p = pos['tp']
                    elif fast[i-1] <= slow[i-1] and fast[i] > slow[i]: exit_p = c[i]
                
                if exit_p:
                    slip = (1 - SLIPPAGE) if pos['side'] == 'long' else (1 + SLIPPAGE)
                    exit_p *= slip
                    pnl_raw = (exit_p - pos['entry'])/pos['entry'] if pos['side']=='long' else (pos['entry']-exit_p)/pos['entry']
                    pnl = pnl_raw * CAPITAL * POS_SIZE - CAPITAL * POS_SIZE * FEE * 2
                    result.trades.append({'pnl': pnl})
                    equity += pnl
                    result.equity.append(equity)
                    pos = None
                    continue
            
            # Entry
            if pos is None:
                bull = fast[i-1] <= slow[i-1] and fast[i] > slow[i]
                bear = fast[i-1] >= slow[i-1] and fast[i] < slow[i]
                rsi_ok = params['rsi_lo'] < rsi_vals[i] < params['rsi_hi']
                trend_ok = True
                if trend is not None and not np.isnan(trend[i]):
                    trend_ok = (c[i] > trend[i]) if bull else (c[i] < trend[i]) if bear else True
                
                sig = None
                if bull and rsi_ok and trend_ok: sig = 'long'
                elif bear and rsi_ok and trend_ok: sig = 'short'
                
                if sig:
                    slip = (1 + SLIPPAGE) if sig == 'long' else (1 - SLIPPAGE)
                    entry = c[i] * slip
                    atr_v = atr_vals[i]
                    if sig == 'long':
                        sl = entry - atr_v * params['atr_sl']
                        tp = entry + atr_v * params['atr_sl'] * params['tp_ratio']
                    else:
                        sl = entry + atr_v * params['atr_sl']
                        tp = entry - atr_v * params['atr_sl'] * params['tp_ratio']
                    pos = {'side': sig, 'entry': entry, 'sl': sl, 'tp': tp}
    
    return result


def main():
    print("=" * 50)
    print("EMA Optimizer")
    print("=" * 50)
    
    data = load_data()
    print(f"Loaded: {list(data.keys())}")
    
    # Baseline
    base_p = {'fast_ema': 9, 'slow_ema': 21, 'rsi_lo': 30, 'rsi_hi': 70, 'atr_sl': 2.0, 'tp_ratio': 2.0, 'trend_ema': 0}
    base = backtest(base_p, data)
    print(f"\nBaseline: {len(base.trades)} trades, {base.win_rate:.1f}% WR, {base.pnl_pct:.2f}% PnL, PF {base.profit_factor:.2f}")
    
    # Grid - simplified
    grid = {
        'fast_ema': [8, 9, 12],
        'slow_ema': [21, 26],
        'rsi_lo': [35, 40, 45],
        'rsi_hi': [55, 60, 65],
        'atr_sl': [1.5, 2.0, 2.5],
        'tp_ratio': [2.0, 2.5, 3.0],
        'trend_ema': [0, 50]
    }
    
    combos = []
    for f, s, rl, rh, atr_s, tp, tr in product(grid['fast_ema'], grid['slow_ema'], grid['rsi_lo'], 
                                                grid['rsi_hi'], grid['atr_sl'], grid['tp_ratio'], grid['trend_ema']):
        if f < s and rl < rh:
            combos.append({'fast_ema': f, 'slow_ema': s, 'rsi_lo': rl, 'rsi_hi': rh, 'atr_sl': atr_s, 'tp_ratio': tp, 'trend_ema': tr})
    
    print(f"\nTesting {len(combos)} combos...")
    
    results = []
    best = None
    best_score = -999
    
    for i, p in enumerate(combos):
        r = backtest(p, data)
        score = r.profit_factor * (r.win_rate/100) if len(r.trades) >= 5 else -999
        if r.pnl_pct < 0: score *= 0.3
        results.append({**p, 'trades': len(r.trades), 'wr': r.win_rate, 'pnl': r.pnl_pct, 'pf': r.profit_factor, 'score': score})
        if score > best_score:
            best_score = score
            best = (p, r)
        if (i+1) % 100 == 0:
            print(f"  {i+1}/{len(combos)}")
    
    # Save
    df = pd.DataFrame(results).sort_values('score', ascending=False)
    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(exist_ok=True)
    df.to_csv(results_dir / "optimization_results.csv", index=False)
    
    print("\n" + "=" * 50)
    print("TOP 5")
    print("=" * 50)
    for _, row in df.head(5).iterrows():
        print(f"  EMA {int(row['fast_ema'])}/{int(row['slow_ema'])}, RSI {int(row['rsi_lo'])}-{int(row['rsi_hi'])}, ATR {row['atr_sl']}, TP {row['tp_ratio']}, Trend {int(row['trend_ema'])}")
        print(f"    → {int(row['trades'])} trades, {row['wr']:.1f}% WR, {row['pnl']:.2f}% PnL")
    
    # Best
    best_p, best_r = best
    print("\n" + "=" * 50)
    print("WINNER")
    print("=" * 50)
    for k, v in best_p.items():
        print(f"  {k}: {v}")
    print(f"\n  {len(best_r.trades)} trades, {best_r.win_rate:.1f}% WR, {best_r.pnl_pct:.2f}% PnL, {best_r.profit_factor:.2f} PF")
    
    # Before vs After
    print("\n" + "-" * 50)
    print("BEFORE vs AFTER")
    print("-" * 50)
    print(f"Win Rate:   {base.win_rate:.1f}% → {best_r.win_rate:.1f}% ({best_r.win_rate - base.win_rate:+.1f}%)")
    print(f"PnL:        {base.pnl_pct:.2f}% → {best_r.pnl_pct:.2f}% ({best_r.pnl_pct - base.pnl_pct:+.2f}%)")
    print(f"PF:         {base.profit_factor:.2f} → {best_r.profit_factor:.2f}")
    
    # Chart
    fig, ax = plt.subplots(2, 2, figsize=(12, 8))
    fig.suptitle('EMA Optimization Results', fontweight='bold')
    
    ax[0,0].plot(base.equity, label='Before', color='red', alpha=0.7)
    ax[0,0].plot(best_r.equity, label='After', color='green', alpha=0.7)
    ax[0,0].axhline(CAPITAL, color='gray', linestyle='--', alpha=0.5)
    ax[0,0].set_title('Equity Curves')
    ax[0,0].legend()
    ax[0,0].grid(True, alpha=0.3)
    
    metrics = ['Win Rate', 'PnL %', 'Profit Factor']
    before = [base.win_rate, base.pnl_pct, base.profit_factor]
    after = [best_r.win_rate, best_r.pnl_pct, best_r.profit_factor]
    x = np.arange(3)
    ax[0,1].bar(x - 0.2, before, 0.4, label='Before', color='red', alpha=0.7)
    ax[0,1].bar(x + 0.2, after, 0.4, label='After', color='green', alpha=0.7)
    ax[0,1].set_xticks(x)
    ax[0,1].set_xticklabels(metrics)
    ax[0,1].set_title('Metrics')
    ax[0,1].legend()
    ax[0,1].axhline(0, color='gray', alpha=0.3)
    ax[0,1].grid(True, alpha=0.3, axis='y')
    
    if best_r.trades:
        pnls = [t['pnl'] for t in best_r.trades]
        colors = ['green' if p > 0 else 'red' for p in pnls]
        ax[1,0].bar(range(len(pnls)), pnls, color=colors, alpha=0.7)
        ax[1,0].axhline(0, color='gray', alpha=0.5)
        ax[1,0].set_title('Trade PnL ($)')
        ax[1,0].grid(True, alpha=0.3, axis='y')
    
    ax[1,1].axis('off')
    txt = f"""OPTIMIZED PARAMETERS

Fast EMA: {best_p['fast_ema']}
Slow EMA: {best_p['slow_ema']}
RSI Range: {best_p['rsi_lo']}-{best_p['rsi_hi']}
ATR SL Mult: {best_p['atr_sl']}
TP:SL Ratio: {best_p['tp_ratio']}
Trend EMA: {best_p['trend_ema']} {'(off)' if best_p['trend_ema']==0 else ''}

RESULTS
Trades: {len(best_r.trades)}
Win Rate: {best_r.win_rate:.1f}%
Total PnL: {best_r.pnl_pct:.2f}%
Profit Factor: {best_r.profit_factor:.2f}"""
    ax[1,1].text(0.1, 0.9, txt, transform=ax[1,1].transAxes, fontsize=10, va='top', 
                 fontfamily='monospace', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    plt.savefig(results_dir / "optimized_comparison.png", dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"\n✓ {results_dir / 'optimization_results.csv'}")
    print(f"✓ {results_dir / 'optimized_comparison.png'}")
    
    return best_p


if __name__ == "__main__":
    best = main()
