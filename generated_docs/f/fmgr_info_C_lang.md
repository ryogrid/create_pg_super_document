# fmgr_info_C_lang

## Location
[src/backend/utils/fmgr/fmgr.c:349-417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L349-L417)

## Overview
This static function handles special processing for initializing FmgrInfo structures for C-language functions, including caching and loading external shared libraries.

## Definition

```c
static void
fmgr_info_C_lang(Oid functionId, FmgrInfo *finfo, HeapTuple procedureTuple)
```
## Detailed Description
fmgr_info_C_lang specializes in setting up function manager information for C-language functions. It first attempts to find the function in a hash table cache to avoid repeated expensive operations. If not cached, it:

1. Extracts the prosrc (function symbol name) and probin (shared library path) from the pg_proc tuple
2. Loads the external function from the shared library using load_external_function
3. Fetches the function information record using fetch_finfo_record
4. Caches both the function pointer and info record for future use
5. Sets the function address in the FmgrInfo structure based on the API version

The function only supports API version 1 functions and will error on unrecognized versions.

## Parameters / Member Variables
- `functionId`: OID of the function being processed (currently unused in implementation)
- `*finfo`: FmgrInfo structure to be initialized with function address
- `procedureTuple`: HeapTuple from pg_proc catalog containing function metadata
## Dependencies
- Functions called/Symbols referenced:
  - [lookup_C_func](../l/lookup_C_func.md) (check function cache)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md) (get prosrc/probin attributes)
  - TextDatumGetCString (convert Datum to C string)
  - [load_external_function](../l/load_external_function.md) (load shared library function)
  - [fetch_finfo_record](fetch_finfo_record.md) (get function info record)
  - [record_C_func](../r/record_C_func.md) (cache function for future use)
  - [pfree](../p/pfree.md) (free memory)
  - elog (error logging)
- Called from (representative examples):
  - [fmgr_info_cxt_security](fmgr_info_cxt_security.md) (main function info setup)

## Notes and Other Information
- This function is part of PostgreSQL's dynamic loading system for C extensions
- It implements a caching mechanism to avoid repeated library loading overhead
- The function assumes C-language functions always have non-null prosrc and probin values
- Memory management includes freeing temporary strings after use
- Only supports function API version 1, which is the current standard

## Simplified Source

```c
static void fmgr_info_C_lang(Oid functionId, FmgrInfo *finfo, HeapTuple procedureTuple)
{
    CFuncHashTabEntry *hashentry;
    PGFunction user_fn;
    const Pg_finfo_record *inforec;

    // Check if function is already cached
    hashentry = lookup_C_func(procedureTuple);
    if (hashentry) {
        user_fn = hashentry->user_fn;
        inforec = hashentry->inforec;
    } else {
        // Extract function info from pg_proc tuple
        Datum prosrcattr = SysCacheGetAttrNotNull(PROCOID, procedureTuple,
                                                  Anum_pg_proc_prosrc);
        char *prosrcstring = TextDatumGetCString(prosrcattr);

        Datum probinattr = SysCacheGetAttrNotNull(PROCOID, procedureTuple,
                                                  Anum_pg_proc_probin);
        char *probinstring = TextDatumGetCString(probinattr);

        // Load the external function from shared library
        void *libraryhandle;
        user_fn = load_external_function(probinstring, prosrcstring, true,
                                         &libraryhandle);

        // Get function information record
        inforec = fetch_finfo_record(libraryhandle, prosrcstring);

        // Cache for future use
        record_C_func(procedureTuple, user_fn, inforec);

        pfree(prosrcstring);
        pfree(probinstring);
    }

    // Set function address based on API version
    switch (inforec->api_version) {
        case 1:
            finfo->fn_addr = user_fn;
            break;
        default:
            elog(ERROR, "unrecognized function API version: %d",
                 inforec->api_version);
            break;
    }
}
```