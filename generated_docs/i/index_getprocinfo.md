# index_getprocinfo

## Location
src/backend/access/index/indexam.c: 860 - 927

## Overview
Retrieves cached function manager information for index access method support procedures, allowing index AMs to maintain fmgr lookup info for support procedures in the relation cache.

## Definition
```c
FmgrInfo *
index_getprocinfo(Relation irel,
                  AttrNumber attnum,
                  uint16 procnum)
```

## Detailed Description
This function provides index access methods with cached FmgrInfo structures for support procedures. It leverages the relation cache to store function manager lookup information, avoiding repeated lookups for "default" functions associated with indexed attributes. The function calculates the appropriate index into the cached support information array based on the attribute number and procedure number, then initializes the FmgrInfo structure if this is the first access. During initialization, it retrieves the procedure OID from the cached support procedure array, validates it exists, and sets up the function manager info along with any opclass-specific options.

The returned pointer references cached data that becomes invalid during relcache rebuilds, so callers must either use the information immediately or acquire appropriate locks on the index relation to ensure cache stability.

## Parameters / Member Variables
- `irel`: Index relation containing the cached support procedure information
- `attnum`: Attribute number (1-based) for which to retrieve the support procedure
- `procnum`: Support procedure number (1-based) within the access method's support procedure set

## Dependencies
- Functions called/Symbols referenced:
  - RegProcedureIsValid
  - fmgr_info_cxt
  - RelationGetIndexAttOptions
  - set_fn_opclass_options
- Called from (representative examples):
  - initGinState (GIN index initialization)
  - initGISTstate (GiST index initialization)
  - _bt_mkscankey (B-tree scan key creation)
  - spgGetCache (SP-GiST cache management)

## Notes and Other Information
- Only caches "default" functions for indexed attributes, not all possible support procedures
- Validates that support functions exist and raises an error if a required function is missing
- Handles opclass options initialization for non-options procedures
- The function assumes IndexSupportInitialize has already populated the rd_support array
- Critical for performance as it avoids repeated function lookups during index operations