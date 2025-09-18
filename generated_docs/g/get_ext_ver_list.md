# get_ext_ver_list

## Location
src/backend/commands/extension.c: 1204 - 1266

## Overview
Scans the extension script directory to build a comprehensive list of ExtensionVersionInfo structures representing all available extension versions and their upgrade paths.

## Definition
```c
static List *get_ext_ver_list(ExtensionControlFile *control)
```

## Detailed Description
This function analyzes an extension's script directory to discover all available versions and upgrade paths by parsing script filenames. It creates a graph-like structure where each ExtensionVersionInfo node contains a list of versions reachable in one upgrade step.

The function processes two types of script files:
1. Install scripts: Named 'extname--version.sql' - these define installable versions
2. Update scripts: Named 'extname--fromver--tover.sql' - these define upgrade paths between versions

For each valid script file found, it creates ExtensionVersionInfo structures and links them appropriately to build the complete version dependency graph that will later be used by path-finding algorithms.

## Parameters / Member Variables
- `control`: ExtensionControlFile containing extension metadata including the extension name used for script filename matching

## Dependencies
- Functions called/Symbols referenced:
  - get_extension_script_directory
  - AllocateDir
  - ReadDir 
  - FreeDir
  - is_extension_script_filename
  - get_ext_ver_info
  - pstrdup
  - strrchr
  - strstr
  - lappend
- Called from (representative examples):
  - identify_update_path
  - CreateExtensionInternal
  - get_available_versions_for_extension
  - pg_extension_update_paths

## Notes and Other Information
- Static function only used within extension.c module
- Handles both install scripts (single version) and update scripts (version transitions)
- Ignores malformed filenames with more than two '--' separators
- Creates bidirectional relationships in the version graph through the 'reachable' lists
- Essential for building the complete extension version dependency graph used by Dijkstra's algorithm
- Script directory location obtained from extension control file