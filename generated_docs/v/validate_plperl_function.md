# validate_plperl_function

## Location
[src/pl/plperl/plperl.c:2671-2699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L2671-L2699)

## Overview
Validates whether a cached PL/Perl function descriptor is still up-to-date by checking transaction ID and tuple ID against the current pg_proc entry.

## Definition

```c
static bool
validate_plperl_function(plperl_proc_ptr *proc_ptr, HeapTuple procTup)
```
## Detailed Description
This function implements cache validation for compiled PL/Perl functions. It checks if a previously compiled function descriptor is still valid by comparing the transaction ID (xmin) and tuple ID from when the function was originally compiled against the current pg_proc tuple. This validation is crucial because CREATE OR REPLACE FUNCTION can modify a function's definition without changing its OID, making cached compiled versions obsolete. If the cached version is outdated, the function unlinks it from the hash table and decrements its reference count.

## Parameters / Member Variables
- `*proc_ptr`: Pointer to the procedure pointer structure containing the cached function descriptor
- `procTup`: Heap tuple from pg_proc containing the current function definition
## Dependencies
- Functions called/Symbols referenced:
  - [plperl_proc_ptr](../p/plperl_proc_ptr.md): Structure type for procedure pointer management
  - [plperl_proc_desc](../p/plperl_proc_desc.md): Structure type for procedure descriptor
  - HeapTupleHeaderGetRawXmin: Gets transaction ID from tuple header
  - [ItemPointerEquals](../I/ItemPointerEquals.md): Compares tuple IDs for equality
  - decrement_prodesc_refcount: Decrements reference count and potentially frees descriptor
- Called from:
  - [compile_plperl_function](../c/compile_plperl_function.md): Used during function compilation/retrieval process

## Notes and Other Information
- Returns true if the cached function is still valid, false if it needs recompilation
- Handles the case where CREATE OR REPLACE FUNCTION changes function definition
- Implements proper reference counting to prevent memory leaks
- Part of the PL/Perl function caching mechanism for performance optimization
- Located at src/pl/plperl/plperl.c:2671-2699