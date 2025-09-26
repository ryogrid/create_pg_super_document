# tuplestore_putvalues

## Location
[src/backend/utils/sort/tuplestore.c:750-764](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L750-L764)

## Overview
A function that constructs and stores a tuple from separate arrays of values and null indicators, avoiding the overhead of intermediate tuple construction.

## Definition
```c
void tuplestore_putvalues(Tuplestorestate *state, TupleDesc tdesc,
                         const Datum *values, const bool *isnull)
```

## Detailed Description
This function provides an efficient way to store tuple data when the values are already available as separate arrays rather than as a constructed tuple. It eliminates the intermediate step of creating a HeapTuple or filling a TupleTableSlot, making it more efficient than tuplestore_puttuple() for scenarios where data originates from value arrays.

The function operates by:
1. Switching to the tuplestore's memory context for proper memory management
2. Constructing a MinimalTuple directly from the value and null arrays using heap_form_minimal_tuple
3. Tracking the memory usage of the created tuple with USEMEM
4. Delegating the actual storage to tuplestore_puttuple_common
5. Restoring the original memory context

This approach is particularly useful for table functions, system information functions, and other scenarios where data is naturally available as arrays of Datum values and null flags rather than constructed tuples.

## Parameters / Member Variables
- `state`: Pointer to the Tuplestorestate structure representing the tuplestore
- `tdesc`: TupleDesc describing the structure and types of the tuple to be created
- `values`: Array of Datum values for each column of the tuple
- `isnull`: Array of boolean flags indicating which columns are NULL

## Dependencies
- Functions called/Symbols referenced:
  - heap_form_minimal_tuple
  - GetMemoryChunkSpace
  - USEMEM
  - tuplestore_puttuple_common
- Types used:
  - Tuplestorestate
  - TupleDesc
  - MinimalTuple
  - Datum
- Called from (representative examples):
  - ExecMakeTableFunctionResult (execSRF.c)
  - pg_timezone_names (datetime.c)
  - pg_config (pg_config.c)
  - Various system information functions

## Notes and Other Information
- More efficient than tuplestore_puttuple() when data is already in array form
- Avoids the overhead of intermediate tuple construction operations
- Widely used by system information functions and table functions
- The created tuple is a MinimalTuple for space efficiency
- Memory allocation occurs in the tuplestore's context
- Maintains the same read pointer behavior as other tuplestore put functions