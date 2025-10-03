# fmgr_info_other_lang

## Location
[src/backend/utils/fmgr/fmgr.c:418-454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L418-L454)

## Overview
This static function handles special processing for initializing FmgrInfo structures for functions written in procedural languages other than C (e.g., PL/pgSQL, PL/Python).

## Definition

```c
static void
fmgr_info_other_lang(Oid functionId, FmgrInfo *finfo, HeapTuple procedureTuple)
```
## Detailed Description
fmgr_info_other_lang sets up function manager information for procedural language functions by:

1. Extracting the language OID from the procedure's pg_proc tuple
2. Looking up the language's metadata in the pg_language system catalog
3. Finding the language's call handler function using its lanplcallfoid
4. Setting up the FmgrInfo for the call handler while bypassing security checks
5. Copying the call handler's function address to the target FmgrInfo

The function deliberately ignores security attributes when setting up the call handler to get a direct pointer to the C-language call handler function. This ensures that procedural language functions are handled through their appropriate language-specific call handlers.

## Parameters / Member Variables
- `functionId`: OID of the function being processed (currently unused in implementation)
- `*finfo`: FmgrInfo structure to be initialized with the call handler address
- `procedureTuple`: HeapTuple from pg_proc catalog containing the procedural language function's metadata
## Dependencies
- Functions called/Symbols referenced:
  - GETSTRUCT (extract struct from HeapTuple)
  - [SearchSysCache1](../S/SearchSysCache1.md) (lookup language in pg_language)
  - HeapTupleIsValid (validate tuple)
  - elog (error logging)
  - [fmgr_info_cxt_security](fmgr_info_cxt_security.md) (setup call handler FmgrInfo)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (release cached tuple)
- Called from (representative examples):
  - [fmgr_info_cxt_security](fmgr_info_cxt_security.md) (main function info setup)

## Notes and Other Information
- This function is part of PostgreSQL's multi-language function support system
- It acts as a bridge between procedural language functions and their C-based call handlers
- The security bypass (true parameter to fmgr_info_cxt_security) is intentional to avoid double-wrapping
- Each procedural language has its own call handler that interprets and executes functions in that language
- The function assumes the language exists in pg_language and has a valid call handler

## Simplified Source

```c
static void fmgr_info_other_lang(Oid functionId, FmgrInfo *finfo, HeapTuple procedureTuple)
{
    Form_pg_proc procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);
    Oid language = procedureStruct->prolang;
    HeapTuple languageTuple;
    Form_pg_language languageStruct;
    FmgrInfo plfinfo;

    // Look up the language in pg_language catalog
    languageTuple = SearchSysCache1(LANGOID, ObjectIdGetDatum(language));
    if (!HeapTupleIsValid(languageTuple))
        elog(ERROR, "cache lookup failed for language %u", language);

    languageStruct = (Form_pg_language) GETSTRUCT(languageTuple);

    // Get the language's call handler function
    // Bypass security to get direct pointer to C function
    fmgr_info_cxt_security(languageStruct->lanplcallfoid, &plfinfo,
                           CurrentMemoryContext, true);

    // Copy call handler address to target FmgrInfo
    finfo->fn_addr = plfinfo.fn_addr;

    ReleaseSysCache(languageTuple);
}
```