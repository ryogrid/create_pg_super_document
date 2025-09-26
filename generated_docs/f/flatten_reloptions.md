# flatten_reloptions

## Location
src/backend/utils/adt/ruleutils.c: 13313 - 13345

## Overview
A static utility function that retrieves and formats the reloptions (relation options) for a given relation OID into a C string representation.

## Definition
```c
static char *
flatten_reloptions(Oid relid)
```

## Detailed Description
This function looks up a relation in the system cache by its OID and extracts the reloptions attribute from the pg_class catalog. If reloptions exist for the relation, it uses the `get_reloptions()` function to format them into a comma-separated, properly quoted string suitable for SQL output. The function performs proper error handling for invalid relation OIDs and returns NULL if no reloptions are defined for the relation. This is commonly used when reconstructing DDL statements for relations that have storage parameters or other options.

## Parameters / Member Variables
- `relid`: The OID of the relation whose reloptions should be retrieved and formatted

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - HeapTupleIsValid
  - elog
  - ObjectIdGetDatum
  - SysCacheGetAttr
  - initStringInfo
  - get_reloptions
  - ReleaseSysCache
- Called from (representative examples):
  - pg_get_indexdef_worker
  - pg_get_constraintdef_worker

## Notes and Other Information
- This is a static function within ruleutils.c, used for SQL object definition reconstruction
- Returns NULL if the relation has no reloptions defined, allowing callers to handle this case appropriately
- Performs proper system cache management with SearchSysCache1/ReleaseSysCache pairing
- Uses error logging (elog) for invalid relation OIDs, which would indicate a serious system inconsistency
- The returned string is allocated in the current memory context and should be managed by the caller
- Accesses the Anum_pg_class_reloptions attribute from the pg_class system catalog