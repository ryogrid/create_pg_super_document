# plperl_return_next_internal

## Location
[src/pl/plperl/plperl.c:3275-3403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L3275-L3403)

## Overview
Internal function that handles the core logic of return_next functionality for PL/Perl SETOF functions, managing tuple storage and memory context.

## Definition
```c
static void plperl_return_next_internal(SV *sv)
```

## Detailed Description
This function implements the internal logic for PL/Perl's return_next functionality, which allows set-returning functions to yield one result at a time. It handles both composite and scalar return types, managing the creation and population of a tuple store that accumulates results across multiple return_next calls within a single function invocation.

The function performs several key operations:
1. Validates that the function is declared as SETOF
2. On first call, determines the output tuple type and creates a tuple store
3. Manages memory contexts to prevent memory leaks during repeated calls
4. Converts Perl values to PostgreSQL tuples and stores them in the tuple store
5. Handles both composite types (hash references) and scalar types

## Parameters / Member Variables
- `sv`: Perl scalar value (SV*) to be returned as the next result tuple. Can be NULL, a hash reference for composite types, or a scalar for simple types.

## Dependencies
- Functions called/Symbols referenced:
  - [get_call_result_type](../g/get_call_result_type.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - tuplestore_begin_heap
  - AllocSetContextCreate
  - [plperl_build_tuple_result](plperl_build_tuple_result.md)
  - [domain_check](../d/domain_check.md)
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md)
  - tuplestore_puttuple
  - [plperl_sv_to_datum](plperl_sv_to_datum.md)
  - tuplestore_putvalues
  - [MemoryContextReset](../M/MemoryContextReset.md)
- Called from (representative examples):
  - [plperl_func_handler](plperl_func_handler.md) (src/pl/plperl/plperl.c:2477)
  - [plperl_return_next](plperl_return_next.md) (src/pl/plperl/plperl.c:3253)

## Notes and Other Information
- Function reports errors via PostgreSQL's ereport mechanism
- Uses temporary memory contexts to prevent memory accumulation during repeated calls
- Handles both composite return types (expecting hash references) and scalar types
- Supports domain types over composite types with proper validation
- The tuple store is created in the query's per-query memory context for persistence
- Memory management includes automatic cleanup of temporary allocations after each call