# ExecHashJoinGetSavedTuple

## Location
src/backend/executor/nodeHashjoin.c: 1356 - 1394

## Overview
Reads the next tuple from a batch file during hash join operations, retrieving both the hash value and tuple data that were previously saved by ExecHashJoinSaveTuple.

## Definition
```c
static TupleTableSlot *ExecHashJoinGetSavedTuple(HashJoinState *hjstate, BufFile *file, uint32 *hashvalue, TupleTableSlot *tupleSlot)
```

## Detailed Description
This function reads tuples that were previously spilled to disk during hash join batch processing. It implements the counterpart to ExecHashJoinSaveTuple by reading the specific file format: a 32-bit hash value followed by a MinimalTuple. The function efficiently reads both the hash value and tuple length in a single operation since both are uint32 values, handles end-of-file conditions gracefully, allocates memory for the tuple data, and stores the reconstructed tuple in the provided slot. It includes interrupt checking since it serves as an alternative code path to normal ExecProcNode() execution.

## Parameters / Member Variables
- `hjstate`: The HashJoinState containing execution context (currently used primarily for consistency)
- `file`: The BufFile to read the tuple from (previously created by ExecHashJoinSaveTuple)
- `hashvalue`: Output parameter that receives the tuple's hash value
- `tupleSlot`: The TupleTableSlot where the reconstructed tuple will be stored

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - BufFileReadMaybeEOF
  - ExecClearTuple
  - [palloc](../p/palloc.md)
  - BufFileReadExact
  - [ExecForceStoreMinimalTuple](ExecForceStoreMinimalTuple.md)
- Called from (representative examples):
  - [ExecHashJoinOuterGetTuple](ExecHashJoinOuterGetTuple.md)
  - [ExecHashJoinNewBatch](ExecHashJoinNewBatch.md)

## Notes and Other Information
- Returns the provided tupleSlot on success, NULL on end-of-file
- Uses BufFileReadMaybeEOF for the header to handle EOF gracefully, then BufFileReadExact for the tuple data
- Allocates memory for the tuple using palloc - caller is responsible for memory context management
- The function is static and used internally within hash join batch processing
- Includes interrupt checking for responsiveness during potentially long file reading operations
- Critical for reconstructing spilled tuples during multi-batch hash join execution