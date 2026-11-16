#!/bin/sh
# Run tinyshed's unit tests (stdlib unittest; temp TINYSHED_HOME, never the live ~/.tinyshed).
#   ./run_tests.sh               all tests, verbose
#   ./run_tests.sh -k Ingest     only tests whose name contains Ingest (case-sensitive; any unittest options)
set -e
cd "$(dirname "$0")"
[ $# -eq 0 ] && set -- -v
exec python3 -W error::ResourceWarning -m unittest "$@" test_tinyshed
