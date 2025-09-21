#! /bin/bash
# this script should be run in the clone of any-script-mcp repo
CURRENTPATH=$(pwd)

export ANY_SCRIPT_MCP_CONFIG="${CURRENTPATH}/../any_script_mcp/config.yml"
claude mcp add -s user any-script -- npx any-script-mcp