# record_C_func

## Location
[src/backend/utils/fmgr/fmgr.c:539-579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L539-L579)

## Overview
Records or updates information about a C function in PostgreSQL's internal CFuncHash hash table, maintaining metadata for efficient function lookup and execution.

## Definition

```c
static void
record_C_func(HeapTuple procedureTuple,
			  PGFunction user_fn, const Pg_finfo_record *inforec)
```
## Detailed Description
The  function is a critical internal function in PostgreSQL's function manager (fmgr) system that maintains a hash table of C-language functions. This function either creates a new entry or updates an existing entry in the CFuncHash table for a given PostgreSQL function OID. The hash table serves as a cache to avoid repeatedly looking up function metadata from the system catalogs during function execution.

The function first ensures the CFuncHash table exists (creating it if necessary), then searches for or creates an entry keyed by the function's OID. Each entry stores essential metadata including the transaction ID when the function was defined, its tuple identifier, the actual C function pointer, and PostgreSQL function information record.

## Parameters / Member Variables
- `procedureTuple`: HeapTuple containing the pg_proc catalog row for this function
- `user_fn`: PGFunction pointer to the actual C function implementation
- `*inforec`: Pg_finfo_record pointer containing PostgreSQL-specific function metadata
## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md)
  - [hash_search](../h/hash_search.md)
  - HeapTupleHeaderGetRawXmin
  - GETSTRUCT (macro)
  - HASH_ENTER
  - HASH_ELEM
  - HASH_BLOBS
- Called from (representative examples):
  - [fmgr_info_C_lang](../f/fmgr_info_C_lang.md)

## Notes and Other Information
- This is a static function, only accessible within fmgr.c
- The CFuncHash table is created with an initial size of 100 entries
- The function uses PostgreSQL's generic hash table implementation
- Transaction visibility information (fn_xmin, fn_tid) is stored to handle catalog changes
- The hash key is the function OID, enabling O(1) lookup performance

## Simplified Source

```c
static void
record_C_func(HeapTuple procedureTuple,
              PGFunction user_fn, const Pg_finfo_record *inforec)
{
    Oid fn_oid = ((Form_pg_proc) GETSTRUCT(procedureTuple))->oid;
    CFuncHashTabEntry *entry;
    bool found;

    // Create hash table if it doesn't exist
    if (CFuncHash == NULL)
    {
        HASHCTL hash_ctl;
        hash_ctl.keysize = sizeof(Oid);
        hash_ctl.entrysize = sizeof(CFuncHashTabEntry);
        CFuncHash = hash_create("CFuncHash", 100, &hash_ctl,
                               HASH_ELEM | HASH_BLOBS);
    }

    // Insert or find existing entry
    entry = (CFuncHashTabEntry *) hash_search(CFuncHash, &fn_oid,
                                              HASH_ENTER, &found);

    // Store function metadata
    entry->fn_xmin = HeapTupleHeaderGetRawXmin(procedureTuple->t_data);
    entry->fn_tid = procedureTuple->t_self;
    entry->user_fn = user_fn;
    entry->inforec = inforec;
}
```