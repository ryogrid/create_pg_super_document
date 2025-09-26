# tuplestore_begin_heap

## Location
[src/backend/utils/sort/tuplestore.c:318-358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L318-L358)

## Overview
Public API function to create a new tuplestore specifically for heap tuples, providing the main entry point for most tuplestore operations in PostgreSQL.

## Definition

```c
Tuplestorestate *
tuplestore_begin_heap(bool randomAccess, bool interXact, int maxKBytes)
```
## Detailed Description
This function creates a new tuplestore optimized for heap tuples, which are the standard row storage format in PostgreSQL tables. It serves as the primary public interface for creating tuplestores and is widely used throughout the codebase for temporary tuple storage needs.

The function configures execution flags based on the randomAccess parameter to determine scanning capabilities, then calls tuplestore_begin_common for basic initialization. After common setup, it installs heap-specific function pointers for tuple operations (copy, write, read), completing the tuplestore configuration for heap tuple handling.

## Parameters / Member Variables
- : Boolean flag enabling both forward and backward tuple access when true; forward-only when false
- : Boolean flag indicating whether the tuplestore should persist beyond the current transaction boundary
- : Maximum memory allocation in kilobytes before spilling tuples to disk storage

## Dependencies
- Functions called/Symbols referenced:
  - tuplestore_begin_common (common initialization)
  - copytup_heap (heap tuple copy function)
  - writetup_heap (heap tuple write function)
  - readtup_heap (heap tuple read function)
  - EXEC_FLAG_BACKWARD (backward scan capability flag)
  - EXEC_FLAG_REWIND (rewind capability flag)
- Data structures used:
  - Tuplestorestate (main tuplestore state structure)
- Called from (representative examples):
  - ExecMaterial (materialization executor node)
  - ExecMakeTableFunctionResult (table function execution)
  - PortalCreateHoldStore (holdable cursor storage)
  - plperl_return_next_internal (PL/Perl function returns)
  - MakeTransitionCaptureState (trigger transition tables)

## Notes and Other Information
- This is the standard public API for creating tuplestores in PostgreSQL
- The randomAccess parameter affects performance: enabling it adds overhead but allows bidirectional scanning
- For interXact=true, callers must ensure the memory context and resource owner also survive transaction boundaries
- The maxKBytes parameter should typically be set to work_mem for optimal memory usage
- Currently the only implemented tuplestore type, though the architecture supports other tuple formats
- Extensively used in executor nodes, PL functions, triggers, and other components requiring temporary tuple storage