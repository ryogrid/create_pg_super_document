# TSConfigIsVisibleExt

## Location
src/backend/catalog/namespace.c: 3222 - 3300

## Overview
An extended version of TSConfigIsVisible that determines whether a text search configuration is visible in the current search path, with optional error handling for missing configurations.

## Definition
```c
static bool TSConfigIsVisibleExt(Oid cfgid, bool *is_missing)
```

## Detailed Description
TSConfigIsVisibleExt performs the core logic for determining text search configuration visibility. It looks up the configuration in the system catalog, recomputes the namespace search path, and checks whether the configuration would be found when searching by its unqualified name. The function handles name conflicts by checking if other configurations with the same name appear earlier in the search path. If is_missing is provided, the function can return false for missing configurations instead of throwing an error.

## Parameters / Member Variables
- `cfgid`: The OID of the text search configuration to check for visibility
- `is_missing`: Optional pointer to bool flag; if not NULL and configuration is missing, sets *is_missing = true and returns false instead of throwing an error

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - HeapTupleIsValid
  - GETSTRUCT
  - recomputeNamespacePath
  - list_member_oid
  - SearchSysCacheExists2
  - ReleaseSysCache
- Called from (representative examples):
  - TSConfigIsVisible
  - pg_ts_config_is_visible

## Notes and Other Information
- This is a static function, only accessible within namespace.c
- Performs a two-phase visibility check: first checks if the namespace is in the search path, then checks for name conflicts
- Configurations in PG_CATALOG_NAMESPACE are always considered to be in the path
- Skips temporary namespace during visibility checks
- The function carefully handles the search path order to determine which configuration would be found first
- Located in src/backend/catalog/namespace.c:3222-3300