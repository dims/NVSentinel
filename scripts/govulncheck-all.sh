#!/usr/bin/env bash
#
# Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# govulncheck-all.sh - Run govulncheck on all Go modules in the repository
#
# This script automatically discovers all Go modules and runs vulnerability
# checks on each one. It distinguishes between actionable vulnerabilities
# (those with fixes available) and non-actionable ones (no fixes available).
#
# Usage:
#   ./scripts/govulncheck-all.sh [--fail-on-any]
#
# Options:
#   --fail-on-any    Fail even for non-actionable vulnerabilities (default: false)
#
# Exit codes:
#   0 - No actionable vulnerabilities found
#   1 - Actionable vulnerabilities found that need fixing
#   2 - Error in script execution

set -euo pipefail

# Script configuration
FAIL_ON_ANY=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --fail-on-any)
      FAIL_ON_ANY=true
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [--fail-on-any]"
      echo ""
      echo "Run govulncheck on all Go modules in the repository."
      echo ""
      echo "Options:"
      echo "  --fail-on-any    Fail even for non-actionable vulnerabilities"
      echo "  -h, --help       Show this help message"
      echo ""
      echo "The script distinguishes between:"
      echo "  - Actionable vulnerabilities: Have fixes available"
      echo "  - Non-actionable vulnerabilities: No fixes available (marked 'Fixed in: N/A')"
      echo ""
      echo "By default, only actionable vulnerabilities cause the script to fail."
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Use --help for usage information." >&2
      exit 2
      ;;
  esac
done

# Color output for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
  echo -e "${BLUE}ℹ${NC} $*"
}

log_success() {
  echo -e "${GREEN}✅${NC} $*"
}

log_warning() {
  echo -e "${YELLOW}⚠️${NC} $*"
}

log_error() {
  echo -e "${RED}❌${NC} $*"
}

# Check if required tools are available
check_dependencies() {
  local missing_tools=()

  if ! command -v govulncheck >/dev/null 2>&1; then
    missing_tools+=("govulncheck")
  fi

  if ! command -v jq >/dev/null 2>&1; then
    missing_tools+=("jq")
  fi

  if [ ${#missing_tools[@]} -ne 0 ]; then
    log_error "Missing required tools: ${missing_tools[*]}"
    echo ""
    echo "Install missing tools:"
    for tool in "${missing_tools[@]}"; do
      case $tool in
        govulncheck)
          echo "  go install golang.org/x/vuln/cmd/govulncheck@latest"
          ;;
        jq)
          echo "  # macOS: brew install jq"
          echo "  # Ubuntu: apt-get install jq"
          echo "  # Or download from: https://github.com/jqlang/jq/releases"
          ;;
      esac
    done
    exit 2
  fi
}

# Find all Go modules in the repository
find_go_modules() {
  log_info "Discovering Go modules..." >&2

  local go_modules
  go_modules=$(find "$REPO_ROOT" -name "go.mod" -type f -not -path '*/.git/*' -not -path "$REPO_ROOT/go.mod" | sort)

  if [ -z "$go_modules" ]; then
    log_error "No Go modules found in repository"
    exit 2
  fi

  # Filter out any modules that are not proper Go modules
  local valid_modules=""
  while IFS= read -r mod_file; do
    [ -n "$mod_file" ] || continue
    local mod_dir
    mod_dir=$(dirname "$mod_file")

    # Check if it's a valid Go module by trying to get the module name
    # and ensuring it's not "command-line-arguments" (which indicates no go.mod)
    local mod_name
    if mod_name=$(cd "$mod_dir" && go list -m 2>/dev/null) && [ "$mod_name" != "command-line-arguments" ]; then
      if [ -n "$valid_modules" ]; then
        valid_modules="$valid_modules"$'\n'"$mod_file"
      else
        valid_modules="$mod_file"
      fi
    fi
  done <<< "$go_modules"

  if [ -z "$valid_modules" ]; then
    log_error "No valid Go modules found in repository"
    exit 2
  fi

  echo "$valid_modules"
}

# Extract module name using go list -m
get_module_name() {
  local mod_dir="$1"
  local mod_name

  if ! mod_name=$(cd "$mod_dir" && go list -m 2>/dev/null); then
    mod_name="unknown-module"
  fi

  echo "$mod_name"
}

# Run govulncheck on a single module and analyze results
check_module_vulnerabilities() {
  local mod_file="$1"
  local mod_dir
  local mod_name
  local vuln_json
  local vuln_exit_code

  mod_dir=$(dirname "$mod_file")
  mod_name=$(get_module_name "$mod_dir")

  echo ""
  log_info "Checking $mod_name (path: $mod_dir)..."

  # Run govulncheck with JSON output for reliable parsing
  set +e  # Allow govulncheck to fail so we can analyze the results
  vuln_json=$(govulncheck -json -C "$mod_dir" ./... 2>&1)
  vuln_exit_code=$?
  set -e

  # Parse JSON output to distinguish calling vs non-calling vulnerabilities
  local calling_vulns
  local calling_with_fix
  local calling_no_fix

  # Count calling vulnerabilities (vulnerabilities in code that's actually called)
  calling_vulns=$(echo "$vuln_json" | jq -r 'select(.finding) | .finding.osv' 2>/dev/null | sort -u | wc -l | tr -d ' ')

  if [ "$calling_vulns" -eq 0 ]; then
    log_success "$mod_name: No calling vulnerabilities found"
    return 0
  fi

  # Get all calling vulnerability IDs
  local calling_vuln_ids
  calling_vuln_ids=$(echo "$vuln_json" | jq -r 'select(.finding) | .finding.osv' 2>/dev/null | sort -u)

  # Count calling vulnerabilities with fixes available
  calling_with_fix=0
  calling_no_fix=0
  local actionable_ids=""
  local no_fix_ids=""

  while IFS= read -r vuln_id; do
    [ -n "$vuln_id" ] || continue

    # Check if this vulnerability has a fix available
    local has_fix
    has_fix=$(echo "$vuln_json" | jq -r --arg id "$vuln_id" 'select(.osv and .osv.id == $id) | .osv.affected[0].ranges[0].events | map(has("fixed")) | any' 2>/dev/null)

    if [ "$has_fix" = "true" ]; then
      calling_with_fix=$((calling_with_fix + 1))
      if [ -n "$actionable_ids" ]; then
        actionable_ids="$actionable_ids $vuln_id"
      else
        actionable_ids="$vuln_id"
      fi
    else
      calling_no_fix=$((calling_no_fix + 1))
      if [ -n "$no_fix_ids" ]; then
        no_fix_ids="$no_fix_ids $vuln_id"
      else
        no_fix_ids="$vuln_id"
      fi
    fi
  done <<< "$calling_vuln_ids"

  # Report results based on what we found
  if [ "$calling_with_fix" -gt 0 ]; then
    log_error "$mod_name: Found $calling_with_fix actionable calling vulnerability(ies) that need fixing"
    echo ""

    # Show detailed information for each actionable vulnerability
    local vuln_count=0
    while IFS= read -r vuln_id; do
      [ -n "$vuln_id" ] || continue

      # Check if this vulnerability has a fix available
      local has_fix
      has_fix=$(echo "$vuln_json" | jq -r --arg id "$vuln_id" 'select(.osv and .osv.id == $id) | .osv.affected[0].ranges[0].events | map(has("fixed")) | any' 2>/dev/null)

      if [ "$has_fix" = "true" ]; then
        vuln_count=$((vuln_count + 1))

        # Extract vulnerability details
        local summary module_name version fixed_version
        summary=$(echo "$vuln_json" | jq -r --arg id "$vuln_id" 'select(.osv and .osv.id == $id) | .osv.summary' 2>/dev/null)
        module_name=$(echo "$vuln_json" | jq -r --arg id "$vuln_id" 'select(.osv and .osv.id == $id) | .osv.affected[0].package.name' 2>/dev/null)
        version=$(echo "$vuln_json" | jq -r --arg id "$vuln_id" 'select(.osv and .osv.id == $id) | .osv.affected[0].ranges[0].events[] | select(has("introduced")) | .introduced' 2>/dev/null | head -1)
        fixed_version=$(echo "$vuln_json" | jq -r --arg id "$vuln_id" 'select(.osv and .osv.id == $id) | .osv.affected[0].ranges[0].events[] | select(has("fixed")) | .fixed' 2>/dev/null | head -1)

        echo "   Vulnerability #$vuln_count: $vuln_id"
        echo "       $summary"
        echo "     More info: https://pkg.go.dev/vuln/$vuln_id"
        echo "     Module: $module_name"
        if [ -n "$version" ] && [ "$version" != "0" ]; then
          echo "       Found in: $module_name@$version"
        fi
        if [ -n "$fixed_version" ]; then
          echo "       Fixed in: $module_name@$fixed_version"
        fi
        echo ""
      fi
    done <<< "$calling_vuln_ids"

    if [ "$calling_no_fix" -gt 0 ]; then
      echo "   Also found $calling_no_fix non-actionable calling vulnerability(ies) (no fixes available):"

      # Show URLs for non-actionable vulnerabilities
      while IFS= read -r vuln_id; do
        [ -n "$vuln_id" ] || continue

        # Check if this vulnerability has no fix available
        local has_fix
        has_fix=$(echo "$vuln_json" | jq -r --arg id "$vuln_id" 'select(.osv and .osv.id == $id) | .osv.affected[0].ranges[0].events | map(has("fixed")) | any' 2>/dev/null)

        if [ "$has_fix" != "true" ]; then
          echo "     https://pkg.go.dev/vuln/$vuln_id"
        fi
      done <<< "$calling_vuln_ids"
    fi

    return 1
  elif [ "$calling_no_fix" -gt 0 ]; then
    log_warning "$mod_name: Found $calling_no_fix non-actionable calling vulnerability(ies) (no fixes available)"
    echo "   These vulnerabilities are called by your code but have no available fixes:"

    # Show URLs for non-actionable vulnerabilities
    while IFS= read -r vuln_id; do
      [ -n "$vuln_id" ] || continue

      # Check if this vulnerability has no fix available
      local has_fix
      has_fix=$(echo "$vuln_json" | jq -r --arg id "$vuln_id" 'select(.osv and .osv.id == $id) | .osv.affected[0].ranges[0].events | map(has("fixed")) | any' 2>/dev/null)

      if [ "$has_fix" != "true" ]; then
        echo "     https://pkg.go.dev/vuln/$vuln_id"
      fi
    done <<< "$calling_vuln_ids"

    if [ "$FAIL_ON_ANY" = true ]; then
      return 1
    else
      return 0
    fi
  else
    log_success "$mod_name: No calling vulnerabilities found"
    return 0
  fi
}

# Main execution
main() {
  log_info "Starting vulnerability check for all Go modules"
  log_info "Repository: $REPO_ROOT"

  if [ "$FAIL_ON_ANY" = true ]; then
    log_info "Mode: Fail on any vulnerabilities (including non-actionable)"
  else
    log_info "Mode: Fail only on actionable vulnerabilities (default)"
  fi

  # Check dependencies
  check_dependencies

  # Find all Go modules
  local go_modules
  go_modules=$(find_go_modules)

  local total_modules=0
  local failed_modules=()

  # Count modules
  while IFS= read -r mod_file; do
    [ -n "$mod_file" ] && total_modules=$((total_modules + 1))
  done <<< "$go_modules"

  log_info "Found $total_modules Go module(s)"

  # Check each module
  while IFS= read -r mod_file; do
    [ -n "$mod_file" ] || continue

    if ! check_module_vulnerabilities "$mod_file"; then
      local mod_dir
      local mod_name
      mod_dir=$(dirname "$mod_file")
      mod_name=$(get_module_name "$mod_dir")
      failed_modules+=("$mod_name")
    fi
  done <<< "$go_modules"

  # Print summary
  echo ""
  echo "=== SUMMARY ==="
  if [ ${#failed_modules[@]} -eq 0 ]; then
    log_success "All $total_modules module(s) passed vulnerability checks!"
    if [ "$FAIL_ON_ANY" = false ]; then
      echo "   (Non-actionable vulnerabilities with no available fixes are ignored)"
    fi
    exit 0
  else
    if [ "$FAIL_ON_ANY" = true ]; then
      log_error "Vulnerabilities found in ${#failed_modules[@]} of $total_modules module(s):"
    else
      log_error "Actionable vulnerabilities found in ${#failed_modules[@]} of $total_modules module(s):"
    fi

    for module in "${failed_modules[@]}"; do
      echo "  - $module"
    done
    echo ""

    if [ "$FAIL_ON_ANY" = true ]; then
      echo "Please review and address the vulnerabilities in the modules listed above."
    else
      echo "Please review and address the actionable vulnerabilities in the modules listed above."
      echo "Note: Vulnerabilities with no available fixes are ignored unless --fail-on-any is used."
    fi

    exit 1
  fi
}

# Run main function
main "$@"