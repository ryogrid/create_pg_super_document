# fmgr_isbuiltin

## Location
src/backend/utils/fmgr/fmgr.c: 76 - 100

## Overview
A fast lookup function that searches the builtin function table by Oid to determine if a function is a PostgreSQL builtin function and returns its metadata.

## Definition
```c
static const FmgrBuiltin *fmgr_isbuiltin(Oid id)
```

## Detailed Description
fmgr_isbuiltin performs an efficient lookup in PostgreSQL's builtin function table using a function's Oid. It implements a two-stage lookup mechanism: first checking if the Oid is within the valid range of builtin functions, then using an index array to quickly locate the function's metadata. This function is optimized for speed since builtin function lookups are frequent operations in query execution.

The function uses a precomputed index array (fmgr_builtin_oid_index) that maps Oids directly to positions in the builtin function array, avoiding linear searches. If the function is not found or the Oid is out of range, it returns NULL, which will typically trigger an error in the calling code.

## Parameters / Member Variables
- `id`: The Oid (object identifier) of the function to look up in the builtin function table

## Dependencies
- Functions called/Symbols referenced:
  - InvalidOidBuiltinMapping (constant used to indicate invalid mapping)
  - FmgrBuiltin (struct type for builtin function metadata)
  - fmgr_last_builtin_oid (global variable marking the highest builtin function Oid)
  - fmgr_builtin_oid_index (global index array for fast Oid-to-index mapping)
  - fmgr_builtins (global array containing builtin function metadata)
- Called from (representative examples):
  - [fmgr_info_cxt_security](fmgr_info_cxt_security.md) (when setting up function call information with security context)

## Notes and Other Information
- This is a static function, only accessible within the fmgr.c file
- The function implements a critical optimization in PostgreSQL's function management system
- Returns NULL for non-builtin functions, which allows the caller to fall back to catalog lookups
- The lookup is O(1) time complexity due to the precomputed index array
- Part of PostgreSQL's Function Manager (fmgr) subsystem responsible for function call dispatch