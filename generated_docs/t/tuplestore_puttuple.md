# tuplestore_puttuple

## Location
src/backend/utils/sort/tuplestore.c: 730 - 749

## Overview
A standard function that accepts a HeapTuple and appends it to the tuplestore, though it's considered somewhat deprecated in favor of slot-based alternatives.

## Definition
```c
void tuplestore_puttuple(Tuplestorestate *state, HeapTuple tuple)
```

## Detailed Description
This function provides the traditional interface for storing HeapTuple data in a tuplestore. While still functional and widely used, it's considered somewhat deprecated due to the prevalence of slot-based tuple handling in modern PostgreSQL code.

The function operates by:
1. Switching to the tuplestore's memory context for proper memory management
2. Creating a copy of the input HeapTuple using the COPYTUP macro
3. Delegating the actual storage operation to tuplestore_puttuple_common
4. Restoring the original memory context

The tuple is always copied regardless of the storage mode (even in WRITEFILE case), ensuring the caller can safely modify or deallocate the original tuple. The COPYTUP macro handles both the copying and memory usage tracking (via USEMEM), so no separate memory accounting is needed.

Like other tuplestore put functions, it maintains specific read pointer behavior designed for the convenience of Material and CTE scan nodes.

## Parameters / Member Variables
- `state`: Pointer to the Tuplestorestate structure representing the tuplestore
- `tuple`: HeapTuple containing the tuple data to be stored

## Dependencies
- Functions called/Symbols referenced:
  - COPYTUP (macro for copying tuples)
  - tuplestore_puttuple_common
- Types used:
  - Tuplestorestate
  - HeapTuple
- Called from (representative examples):
  - ExecMakeTableFunctionResult (execSRF.c)
  - libpqrcv_processTuples (libpqwalreceiver.c)
  - fill_hba_line (hbafuncs.c)
  - plperl_return_next_internal (plperl.c)

## Notes and Other Information
- Considered somewhat deprecated in favor of tuplestore_puttupleslot for new code
- Still widely used due to the large number of existing callers
- The tuple is always copied, ensuring caller ownership is preserved
- COPYTUP macro includes both copying and memory usage tracking
- Memory operations occur in the tuplestore's context for proper cleanup
- Maintains the same read pointer behavior as other tuplestore put functions