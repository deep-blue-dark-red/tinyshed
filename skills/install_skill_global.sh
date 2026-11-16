#!/usr/bin/env bash
set -euo pipefail

# Install the tshed skill into the agent-neutral global skills directory,
# so any session in any repo can use it via tshed:
#
#   ${AGENTS_DIR:-~/.agents}/skills/tshed/SKILL.md
#
#   ./install_skill_global.sh                    # installs to ~/.agents/skills
#   AGENTS_DIR=/x ./install_skill_global.sh     # installs to /x/skills

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE="$ROOT/tshed"
TARGET_DIR="${AGENTS_DIR:-$HOME/.agents}/skills"

[[ -d "$SOURCE" ]] || { echo "install_skill_global.sh: missing $SOURCE" >&2; exit 1; }

mkdir -p "$TARGET_DIR"
rm -rf "$TARGET_DIR/tshed"
cp -R "$SOURCE" "$TARGET_DIR/"
echo "Installed tshed skill to $TARGET_DIR/tshed"
