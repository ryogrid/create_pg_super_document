# lookup_C_func

## Location
[src/backend/utils/fmgr/fmgr.c:515-538](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L515-L538)

## Overview
This static function searches a hash table cache to find a previously loaded C function entry, verifying that the cached entry is still current.

## Definition

```c
static CFuncHashTabEntry *
lookup_C_func(HeapTuple procedureTuple)
```
## Detailed Description
lookup_C_func implements a caching mechanism for C-language functions to avoid expensive repeated loading operations. The function:

1. Extracts the function OID from the pg_proc tuple
2. Checks if the global CFuncHash table exists
3. Searches the hash table using the function OID as the key
4. Validates that any found entry is still current by comparing:
   - The transaction ID (xmin) when the function was defined
   - The tuple identifier (TID) of the pg_proc row
5. Returns the valid cached entry or NULL if not found/outdated

This validation ensures that cached function pointers remain valid even if the function definition changes in the catalog.

## Parameters / Member Variables
- `procedureTuple`: HeapTuple from pg_proc catalog containing the function's metadata and used for freshness validation
## Dependencies
- Functions called/Symbols referenced:
  - GETSTRUCT (extract struct from HeapTuple)
  - [hash_search](../h/hash_search.md) (search hash table)
  - HeapTupleHeaderGetRawXmin (get transaction ID from tuple header)
  - [ItemPointerEquals](../I/ItemPointerEquals.md) (compare tuple identifiers)
- Called from (representative examples):
  - [fmgr_info_C_lang](../f/fmgr_info_C_lang.md) (during function setup to check cache)

## Notes and Other Information
- This function is part of PostgreSQL's performance optimization for C function loading
- The cache validity check prevents stale function pointers after catalog changes
- The function returns NULL in multiple cases: no hash table, no entry found, or outdated entry
- The CFuncHash table is lazily initialized and may not exist early in session startup
- Transaction ID and TID comparison ensures cache coherency with catalog updates

## Simplified Source

```c
static CFuncHashTabEntry *
lookup_C_func(HeapTuple procedureTuple)
{
    Oid fn_oid = ((Form_pg_proc) GETSTRUCT(procedureTuple))->oid;
    CFuncHashTabEntry *entry;

    // Check if hash table exists
    if (CFuncHash == NULL)
        return NULL;

    // Search for function entry by OID
    entry = (CFuncHashTabEntry *) hash_search(CFuncHash, &fn_oid, HASH_FIND, NULL);
    if (entry == NULL)
        return NULL;

    // Verify cached entry is still current
    if (entry->fn_xmin == HeapTupleHeaderGetRawXmin(procedureTuple->t_data) &&
        ItemPointerEquals(&entry->fn_tid, &procedureTuple->t_self))
        return entry;  // Cache hit - entry is valid

    return NULL;  // Entry is outdated
}
```