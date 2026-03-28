#!/usr/bin/env bash
# security-check.sh — run before every deployment to catch known vulnerabilities
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "================================================"
echo " Axiom Intelligence — Security Check"
echo "================================================"

# ── Python dependency audit ──────────────────────────────────────────────────
echo ""
echo "[1/2] Auditing Python dependencies with pip-audit..."
cd "$ROOT/backend"

if ! command -v pip-audit &>/dev/null; then
  echo "  pip-audit not found — installing..."
  pip install pip-audit --quiet
fi

pip-audit -r requirements.txt --desc || {
  echo ""
  echo "⚠  pip-audit found vulnerabilities. Review above and update affected packages."
  exit 1
}

echo "  ✓ No known Python vulnerabilities found."

# ── npm audit ────────────────────────────────────────────────────────────────
echo ""
echo "[2/2] Auditing npm dependencies..."
cd "$ROOT/frontend"

npm audit --audit-level=high || {
  echo ""
  echo "⚠  npm audit found high/critical vulnerabilities. Run 'npm audit fix' to remediate."
  exit 1
}

echo "  ✓ No high/critical npm vulnerabilities found."

echo ""
echo "================================================"
echo " Security check passed."
echo "================================================"
