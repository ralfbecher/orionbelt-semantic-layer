#!/usr/bin/env bash
# Build MkDocs site and deploy to gh-pages branch.
# Usage: ./scripts/deploy-docs.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Installing docs dependencies..."
uv sync --project "$REPO_ROOT" --extra docs

echo "Building docs..."
uv run --project "$REPO_ROOT" mkdocs build --strict --config-file "$REPO_ROOT/mkdocs.yml"

FILE_COUNT=$(find "$REPO_ROOT/site" -type f | wc -l | tr -d ' ')
echo "Built $FILE_COUNT files"

# Cache-bust custom CSS based on its content hash so style changes propagate
# to repeat visitors without a manual hard-refresh. The query string changes
# only when the CSS content actually changes, so caching still works between
# deploys with no CSS edits.
CSS_FILE="$REPO_ROOT/site/stylesheets/extra.css"
if [ -f "$CSS_FILE" ]; then
  CSS_HASH=$(shasum -a 256 "$CSS_FILE" | cut -c1-10)
  echo "Cache-busting extra.css (v=$CSS_HASH)..."
  # perl -i works the same on macOS and Linux (unlike sed -i).
  # Regex matches first-time references and any prior ?v=<hash>.
  find "$REPO_ROOT/site" -name "*.html" -exec \
    perl -i -pe "s|stylesheets/extra\\.css(\\?v=[a-f0-9]+)?|stylesheets/extra.css?v=$CSS_HASH|g" {} +
fi

# Deploy site/ to gh-pages branch using ghp-import (bundled with mkdocs)
echo "Deploying to gh-pages branch..."
uv run --project "$REPO_ROOT" ghp-import \
  --no-jekyll \
  --push \
  --force \
  --message "Update docs ($(date +%Y-%m-%d))" \
  "$REPO_ROOT/site"

rm -rf "$REPO_ROOT/site"
echo "Done — docs deployed to gh-pages (site/ cleaned up)."
