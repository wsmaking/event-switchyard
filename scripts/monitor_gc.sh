#!/usr/bin/env bash
set -euo pipefail

#
# GC Monitoring Script
#
# JVMプロセスのGC統計をリアルタイムで監視
#
# Usage:
#   ./scripts/monitor_gc.sh [PID]
#

pid="${1:-$(pgrep -f 'app.MainKt' | head -1)}"

if [[ -z "$pid" ]]; then
  echo "ERROR: No Java process found"
  exit 1
fi

echo "==> Monitoring GC for PID: $pid"
echo "  Press Ctrl+C to stop"
echo

# jstat -gcutil: GC統計の割合表示
# S0: Survivor 0使用率
# S1: Survivor 1使用率
# E: Eden使用率
# O: Old Gen使用率
# M: Metaspace使用率
# YGC: Young GC回数
# YGCT: Young GC時間
# FGC: Full GC回数
# FGCT: Full GC時間

jstat -gcutil -t "$pid" 1000
