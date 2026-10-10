#!/usr/bin/env bash
# Gather everything from the Ada runs into one file to send back.
#
#   bash ada/collect.sh          # writes ada/RESULTS.txt and prints it

set -uo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
OUT=ada/RESULTS.txt

{
  echo "GFS on Ada -- collected $(date '+%F %T')"
  echo "host: $(hostname)   user: $USER"
  echo

  echo "=============================================================="
  echo "STILL RUNNING"
  echo "=============================================================="
  squeue -u "$USER" 2>/dev/null || echo "(squeue unavailable)"
  echo

  echo "=============================================================="
  echo "CORRECTNESS"
  echo "=============================================================="
  # The line that matters is the pytest summary; everything else is noise.
  for f in ada/logs/tests-*.out; do
      [ -e "$f" ] || continue
      echo "--- $f"
      grep -E 'passed|failed|error' "$f" | tail -5
      # A job killed by the scheduler leaves no pytest summary at all, which
      # is easy to mistake for "no failures".
      grep -lE 'CANCELLED|TIME LIMIT|OUT_OF_MEMORY' "$f" >/dev/null 2>&1 \
          && echo "    !! job was killed by the scheduler -- results incomplete"
  done
  for f in ada/logs/tests-*.err; do
      [ -e "$f" ] || continue
      if [ -s "$f" ]; then echo "--- $f (stderr, last 20)"; tail -20 "$f"; fi
  done
  echo

  echo "=============================================================="
  echo "BENCHMARKS"
  echo "=============================================================="
  for f in ada/logs/bench*.out; do
      [ -e "$f" ] || continue
      echo "--- $f"
      grep -E 'size_mb=|clients=|cache=|config=|metric=|bench|==|MB/s|FAILED|Error|Traceback' "$f" | tail -40
      echo
  done

  echo "=============================================================="
  echo "RESULT FILES"
  echo "=============================================================="
  for f in bench/results/*.json; do
      [ -e "$f" ] || continue
      echo "--- $f"
      cat "$f"
      echo
  done
} > "$OUT" 2>&1

echo "wrote $OUT ($(wc -l < "$OUT") lines)"
echo
echo "Send it back with:   cat $OUT"
echo "or from your laptop: scp $USER@ada.iiit.ac.in:~/gfs/$OUT ."
