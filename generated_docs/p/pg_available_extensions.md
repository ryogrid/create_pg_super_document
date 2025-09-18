# pg_available_extensions

## Location
src/backend/commands/extension.c: 2008 - 2087

## Overview
This function provides a set-returning function (SRF) that lists all available PostgreSQL extensions by reading and parsing control files from the extension control directory.

## Definition


## Detailed Description
The pg_available_extensions function scans the extension control directory (typically ) and returns information about all available extensions. For each primary control file found (files ending in ), it parses the control file and extracts key metadata including the extension name, default version, and comment.

The function returns a result set with three columns:
1. Extension name
2. Default version (nullable)
3. Comment/description (nullable)

The function specifically ignores auxiliary control files (those containing "--" in the filename) and only processes primary control files. If the control directory doesn't exist, the function silently returns an empty result set rather than throwing an error.

## Parameters / Member Variables
This function uses the PostgreSQL function call convention and doesn't take explicit parameters beyond the standard .

## Dependencies
- Functions called/Symbols referenced:
  - InitMaterializedSRF
  - get_extension_control_directory
  - AllocateDir
  - ReadDir
  - is_extension_control_filename
  - read_extension_control_file
  - DirectFunctionCall1
  - namein
  - CStringGetDatum
  - CStringGetTextDatum
  - tuplestore_putvalues
  - FreeDir
- Called from (representative examples):
  - No direct references found (typically called via SQL function interface)

## Notes and Other Information
- This function is designed to be called from SQL as a set-returning function
- It provides the backend implementation for the pg_available_extensions system view
- The function gracefully handles the case where the extension control directory doesn't exist
- Only primary control files are processed; auxiliary control files (with "--" in the name) are skipped
- The function uses PostgreSQL's tuplestore mechanism to build and return the result set
- Error handling is minimal - most errors are passed through from the underlying directory and file operations