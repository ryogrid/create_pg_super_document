# RestrictSearchPath

## Location
[src/backend/utils/misc/guc.c:2248-2263](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L2248-L2263)

## Overview
RestrictSearchPath sets the search_path to a predefined safe value during maintenance operations to prevent potential security issues and ensure predictable behavior.

## Definition


## Detailed Description
This function temporarily restricts the search_path configuration parameter to a safe, well-known value during critical maintenance operations. This security measure prevents potentially malicious or problematic schema resolution that could occur if user-defined search paths were used during system maintenance tasks. The function only operates outside of bootstrap mode, as the search_path is already fixed and safe during bootstrap processing.

Key behaviors:
- Sets search_path to GUC_SAFE_SEARCH_PATH (typically 'pg_catalog, pg_temp')
- Only operates when not in bootstrap processing mode
- Uses GUC_ACTION_SAVE to ensure the change can be rolled back
- Prevents schema-based security vulnerabilities during maintenance
- Ensures consistent object resolution across different user contexts

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode (checks if in bootstrap mode)
  - set_config_option (sets the configuration parameter)
  - GUC_SAFE_SEARCH_PATH (constant defining the safe search path)
  - PGC_USERSET, PGC_S_SESSION (parameter classification constants)
  - GUC_ACTION_SAVE (action type for transactional setting)
- Called from (representative examples):
  - [index_build](../i/index_build.md) (index construction operations)
  - [reindex_index](../r/reindex_index.md) (index rebuilding operations)
  - [vacuum_rel](../v/vacuum_rel.md) (vacuum operations)
  - [DefineIndex](../D/DefineIndex.md) (index definition operations)
  - [do_analyze_rel](../d/do_analyze_rel.md) (table analysis operations)

## Notes and Other Information
- This is a public function declared in guc.h
- Critical for security during maintenance operations that execute user-defined functions
- The GUC_SAFE_SEARCH_PATH typically includes only 'pg_catalog, pg_temp' to prevent malicious code execution
- Used extensively in DDL operations and maintenance commands
- The GUC_ACTION_SAVE ensures the original search_path is restored when the operation completes
- Prevents search_path-based privilege escalation attacks during system operations
- Does nothing during bootstrap because search_path is already restricted and cannot be changed
- Essential for maintaining security invariants during automated maintenance tasks