# fmgr_info_C_lang

## Location
src/backend/utils/fmgr/fmgr.c: 349 - 417

## Overview
This static function handles special processing for initializing FmgrInfo structures for C-language functions, including caching and loading external shared libraries.

## Definition


## Detailed Description
fmgr_info_C_lang specializes in setting up function manager information for C-language functions. It first attempts to find the function in a hash table cache to avoid repeated expensive operations. If not cached, it:

1. Extracts the prosrc (function symbol name) and probin (shared library path) from the pg_proc tuple
2. Loads the external function from the shared library using load_external_function
3. Fetches the function information record using fetch_finfo_record
4. Caches both the function pointer and info record for future use
5. Sets the function address in the FmgrInfo structure based on the API version

The function only supports API version 1 functions and will error on unrecognized versions.

## Parameters / Member Variables
- : OID of the function being processed (currently unused in implementation)
- : FmgrInfo structure to be initialized with function address
- : HeapTuple from pg_proc catalog containing function metadata

## Dependencies
- Functions called/Symbols referenced:
  - lookup_C_func (check function cache)
  - SysCacheGetAttrNotNull (get prosrc/probin attributes)
  - TextDatumGetCString (convert Datum to C string)
  - load_external_function (load shared library function)
  - fetch_finfo_record (get function info record)
  - record_C_func (cache function for future use)
  - pfree (free memory)
  - elog (error logging)
- Called from (representative examples):
  - fmgr_info_cxt_security (main function info setup)

## Notes and Other Information
- This function is part of PostgreSQL's dynamic loading system for C extensions
- It implements a caching mechanism to avoid repeated library loading overhead
- The function assumes C-language functions always have non-null prosrc and probin values
- Memory management includes freeing temporary strings after use
- Only supports function API version 1, which is the current standard