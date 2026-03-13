#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=""
OUTPUT_TAR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-root)
      PROJECT_ROOT="$2"
      shift 2
      ;;
    --output)
      OUTPUT_TAR="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: package_for_openclaw.sh [--project-root <path>] [--output <bundle.tar.gz>]" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$PROJECT_ROOT" ]]; then
  PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
else
  PROJECT_ROOT="$(cd "$PROJECT_ROOT" && pwd)"
fi

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
BUNDLE_NAME="wq_openclaw_optimizer_bundle_${TIMESTAMP}"

if [[ -z "$OUTPUT_TAR" ]]; then
  mkdir -p "$PROJECT_ROOT/dist"
  OUTPUT_TAR="$PROJECT_ROOT/dist/${BUNDLE_NAME}.tar.gz"
else
  mkdir -p "$(dirname "$OUTPUT_TAR")"
fi

STAGE_DIR="$(mktemp -d)"
BUNDLE_ROOT="$STAGE_DIR/$BUNDLE_NAME"
mkdir -p "$BUNDLE_ROOT"

copy_item() {
  local rel_path="$1"
  local src="$PROJECT_ROOT/$rel_path"
  local dst="$BUNDLE_ROOT/$rel_path"

  if [[ ! -e "$src" ]]; then
    echo "[WARN] missing: $rel_path"
    return
  fi

  mkdir -p "$(dirname "$dst")"
  cp -R "$src" "$dst"
  echo "[OK] copied: $rel_path"
}

copy_item "skills/wq-openclaw-alpha-optimizer"
copy_item "wq_submitter_single.py"
copy_item "wqminer"
copy_item "pyproject.toml"
copy_item "requirements.txt"
copy_item "llm.example.json"
copy_item "credentials.example.json"
copy_item "templates/scraped_templates.json"
copy_item "docs/fast_expr_syntax_manual.json"

cat > "$BUNDLE_ROOT/run_openclaw_round.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 "$ROOT/skills/wq-openclaw-alpha-optimizer/scripts/run_round.py" --workspace "$ROOT" "$@"
EOF
chmod +x "$BUNDLE_ROOT/run_openclaw_round.sh"

cat > "$BUNDLE_ROOT/run_openclaw_loop.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 "$ROOT/skills/wq-openclaw-alpha-optimizer/scripts/run_loop.py" --workspace "$ROOT" "$@"
EOF
chmod +x "$BUNDLE_ROOT/run_openclaw_loop.sh"

tar -C "$STAGE_DIR" -czf "$OUTPUT_TAR" "$BUNDLE_NAME"
rm -rf "$STAGE_DIR"

echo "[OK] bundle ready: $OUTPUT_TAR"
