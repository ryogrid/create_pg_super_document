# ExecHashJoinSaveTuple

## Location
[src/backend/executor/nodeHashjoin.c:1315-1355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L1315-L1355)

## Overview
Saves a tuple to a batch file during hash join operations, storing both the hash value and tuple data for later processing when the tuple doesn't fit in the current batch.

## Definition
```c
void ExecHashJoinSaveTuple(MinimalTuple tuple, uint32 hashvalue, BufFile **fileptr, HashJoinTable hashtable)
```

## Detailed Description
This function is responsible for spilling tuples to disk when they cannot be processed in the current hash join batch due to memory constraints. It implements lazy file creation, where batch files are only created when the first tuple needs to be saved. The function stores tuples in a specific format: first the 32-bit hash value, followed by the complete tuple in MinimalTuple format. Memory management is carefully handled by allocating batch file buffers in the spillCxt context rather than batchCxt, ensuring that file buffers outlive individual batches and providing better accounting of spilling memory consumption.

## Parameters / Member Variables
- `tuple`: The MinimalTuple to be saved to the batch file
- `hashvalue`: The 32-bit hash value associated with the tuple for future batch assignment
- `fileptr`: Pointer to the BufFile pointer for the target batch file (modified if file creation is needed)
- `hashtable`: The HashJoinTable containing memory context information for proper allocation

## Dependencies
- Functions called/Symbols referenced:
  - [BufFileCreateTemp](../B/BufFileCreateTemp.md)
  - [BufFileWrite](../B/BufFileWrite.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from (representative examples):
  - [ExecHashIncreaseNumBatches](ExecHashIncreaseNumBatches.md)
  - [ExecHashTableInsert](ExecHashTableInsert.md)
  - [ExecHashRemoveNextSkewBucket](ExecHashRemoveNextSkewBucket.md)
  - [ExecHashJoinImpl](ExecHashJoinImpl.md)

## Notes and Other Information
- Uses lazy file creation - batch files are only created when the first tuple is written
- Allocates file buffers in spillCxt context for better memory management and accounting
- File format: 4-byte hash value followed by variable-length MinimalTuple data
- Critical for handling hash joins that exceed available memory by spilling data to disk
- The function is declared in nodeHashjoin.h and used across multiple hash join implementation files