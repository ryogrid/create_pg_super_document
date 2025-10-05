# ExecHashJoinGetSavedTuple

## Location
[src/backend/executor/nodeHashjoin.c:1356-1394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L1356-L1394)

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
  - [BufFileReadMaybeEOF](../B/BufFileReadMaybeEOF.md)
  - [ExecClearTuple](ExecClearTuple.md)
  - [palloc](../p/palloc.md)
  - [BufFileReadExact](../B/BufFileReadExact.md)
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

## Simplified Source

```c
static TupleTableSlot *
ExecHashJoinGetSavedTuple(HashJoinState *hjstate,
                          BufFile *file,
                          uint32 *hashvalue,
                          TupleTableSlot *tupleSlot)
{
    uint32 header[2];
    size_t nread;
    MinimalTuple tuple;

    // Check for interrupts since this replaces ExecProcNode() calls
    CHECK_FOR_INTERRUPTS();

    // Read hash value and tuple length in one operation
    nread = BufFileReadMaybeEOF(file, header, sizeof(header), true);
    if (nread == 0) {
        // End of file reached
        ExecClearTuple(tupleSlot);
        return NULL;
    }

    // Extract hash value and allocate tuple memory
    *hashvalue = header[0];
    tuple = (MinimalTuple) palloc(header[1]);
    tuple->t_len = header[1];

    // Read the remaining tuple data
    BufFileReadExact(file,
                     (char *) tuple + sizeof(uint32),
                     header[1] - sizeof(uint32));

    // Store tuple in slot and return
    ExecForceStoreMinimalTuple(tuple, tupleSlot, true);
    return tupleSlot;
}
```