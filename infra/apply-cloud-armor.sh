#!/usr/bin/env bash
# Re-apply Cloud Armor security policy for OrionBelt Semantic Layer.
#
# Usage:
#   ./infra/apply-cloud-armor.sh                    # uses defaults
#   ./infra/apply-cloud-armor.sh my-policy us-east1 # custom policy name & region
#
# Prerequisites: gcloud CLI authenticated with compute.securityPolicies permissions.
set -euo pipefail

POLICY="${1:-default-security-policy-for-backend-service-orionbelt-backend}"
REGION="${2:-europe-west1}"

echo "=== Cloud Armor policy: $POLICY (region: $REGION) ==="

# Helper: create rule if it doesn't exist, update if it does
apply_rule() {
  local priority="$1"; shift
  echo "  Rule $priority ..."
  if gcloud compute security-policies rules describe "$priority" \
      --security-policy="$POLICY" --region="$REGION" &>/dev/null; then
    gcloud compute security-policies rules update "$priority" \
      --security-policy="$POLICY" --region="$REGION" "$@"
  else
    gcloud compute security-policies rules create "$priority" \
      --security-policy="$POLICY" --region="$REGION" "$@"
  fi
}

# --- Custom rules ---

apply_rule 100 \
  --expression="request.headers['user-agent'].lower().matches('.*libredtail.*|.*zgrab.*|.*masscan.*|.*nuclei.*|.*nikto.*|.*sqlmap.*|.*dirbuster.*|.*gobuster.*|.*nmap.*|.*python-requests.*|.*go-http-client.*|.*httpx.*')" \
  --action=deny-403 \
  --description="Block known vulnerability scanners"

apply_rule 101 \
  --expression='request.path.lower().matches(".*[.]php.*")' \
  --action=deny-404 \
  --description="Block all PHP file probes"

apply_rule 102 \
  --expression='request.path.lower().matches(".*containers/json.*|.*/vendor/.*|.*wp-admin.*|.*wp-login.*|.*wp-content.*|.*cgi-bin.*|.*[.]env.*|.*[.]git.*")' \
  --action=deny-404 \
  --description="Block common exploit paths"

apply_rule 103 \
  --expression='has(request.headers["content-length"]) && int(request.headers["content-length"]) > 5242880 && request.path.matches(".*/models$|.*/validate$")' \
  --action=deny-413 \
  --description="Block model/validate bodies > 5 MB"

apply_rule 104 \
  --expression='request.method.matches("PUT|PATCH|TRACE|CONNECT")' \
  --action=deny-403 \
  --description="Block PUT/PATCH/TRACE/CONNECT methods"

apply_rule 106 \
  --expression='has(request.headers["content-length"]) && int(request.headers["content-length"]) > 1048576 && !request.path.matches(".*/models$|.*/validate$")' \
  --action=deny-413 \
  --description="Block request bodies > 1 MB (non-model endpoints)"

# --- Application allow rules (before OWASP) ---

apply_rule 150 \
  --expression='request.path.matches(".*/query/sql$")' \
  --action=allow \
  --description="Allow query compilation endpoint (OWASP falsely flags SQL-related body content)"

# --- OWASP WAF rules ---

apply_rule 200 \
  --expression="evaluatePreconfiguredExpr('scannerdetection-v33-stable', ['owasp-crs-v030301-id913101-scannerdetection', 'owasp-crs-v030301-id913102-scannerdetection'])" \
  --action=deny-403 \
  --description="OWASP scanner detection (sensitivity 1)"

apply_rule 202 \
  --expression="evaluatePreconfiguredExpr('rce-v33-stable', ['owasp-crs-v030301-id932200-rce', 'owasp-crs-v030301-id932106-rce', 'owasp-crs-v030301-id932190-rce'])" \
  --action=deny-403 \
  --description="OWASP RCE protection (sensitivity 1)"

apply_rule 203 \
  --expression="evaluatePreconfiguredExpr('rfi-v33-stable', ['owasp-crs-v030301-id931130-rfi'])" \
  --action=deny-403 \
  --description="OWASP RFI protection (sensitivity 1)"

apply_rule 204 \
  --expression="evaluatePreconfiguredExpr('protocolattack-v33-stable', ['owasp-crs-v030301-id921151-protocolattack', 'owasp-crs-v030301-id921170-protocolattack'])" \
  --action=deny-403 \
  --description="OWASP protocol attack protection (sensitivity 1)"

apply_rule 205 \
  --expression="evaluatePreconfiguredExpr('cve-canary')" \
  --action=deny-403 \
  --description="Known CVE exploits (Log4Shell etc.)"

# --- Application rules ---

apply_rule 1000 \
  --expression="request.path.startsWith('/ui')" \
  --action=allow \
  --description="Allow Gradio UI paths without rate limiting"

# --- Rate limiting (penultimate priority) ---
# Note: throttle rules need special flags, handle separately
echo "  Rule 2147483646 (rate limit) ..."
if gcloud compute security-policies rules describe 2147483646 \
    --security-policy="$POLICY" --region="$REGION" &>/dev/null; then
  gcloud compute security-policies rules update 2147483646 \
    --security-policy="$POLICY" --region="$REGION" \
    --src-ip-ranges="*" \
    --action=throttle \
    --rate-limit-threshold-count=1000 \
    --rate-limit-threshold-interval-sec=60 \
    --conform-action=allow \
    --exceed-action=deny-403 \
    --enforce-on-key=IP \
    --description="Rate limiting: 1000 req/min per IP"
fi

echo ""
echo "=== Done. All rules applied. ==="
echo ""
gcloud compute security-policies describe "$POLICY" \
  --region="$REGION" \
  --format="table(rules:format='table(priority, action, preview, description)')"
