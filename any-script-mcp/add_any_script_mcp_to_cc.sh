#! /bin/bash
# this script should be run in the clone of any-script-mcp repo on our repo

npm install
npm run build
mkdir -p ~/.config/any-script-mcp
cp ../any_script_mcp/config.yaml ~/.config/any-script-mcp/config.yaml
claude mcp add -s user any-script -- npx any-script-mcp
