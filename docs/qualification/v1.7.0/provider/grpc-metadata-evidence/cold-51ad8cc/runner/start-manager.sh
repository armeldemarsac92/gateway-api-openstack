#!/usr/bin/env bash
set -euo pipefail
case "${HIBISCUS_INITIAL_RULES_MODE:?}" in
    enabled) extra=(--octavia-l7policy-initial-rules) ;;
    disabled) extra=() ;;
    *) exit 2 ;;
esac
exec "${0%/*}/manager" "$@" --octavia-backend-h2c --octavia-http-header-exact --octavia-http-request-redirect --openstack-resync-interval=10m "${extra[@]}"
