# hashagg_batch_read

## Location
[src/backend/executor/nodeAgg.c:3010-3058](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L3010-L3058)

## Overview
Reads the next tuple from a batch's logical tape, returning the tuple data along with its associated hash value, or NULL when no more tuples are available.

## Definition
```c
static MinimalTuple hashagg_batch_read(HashAggBatch *batch, uint32 *hashp)
```

## Detailed Description
This function reads spilled tuple data from a logical tape associated with a HashAggBatch. It follows a specific format where each spilled tuple is stored as: hash value (uint32), tuple length (uint32), followed by the tuple data itself. The function reconstructs the MinimalTuple from the tape data and optionally returns the associated hash value.

The function performs careful error checking at each read operation to ensure data integrity and provides detailed error messages if unexpected EOF conditions are encountered. It allocates memory for the tuple and properly reconstructs the MinimalTuple structure with the correct length field.

Reading process:
1. Read the 32-bit hash value
2. Read the 32-bit tuple length  
3. Allocate memory for the MinimalTuple
4. Read the remaining tuple data
5. Return the reconstructed tuple and hash

## Parameters / Member Variables
- `batch`: HashAggBatch structure containing the input tape to read from
- `hashp`: Optional pointer to receive the hash value associated with the tuple (can be NULL if hash is not needed)

## Dependencies
- Functions called/Symbols referenced:
  - LogicalTapeRead
  - [palloc](../p/palloc.md)
  - ereport
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - [errmsg_internal](../e/errmsg_internal.md)
- Called from (representative examples):
  - [agg_refill_hash_table](../a/agg_refill_hash_table.md)

## Notes and Other Information
- Returns NULL when reaching the end of the tape (no more tuples to read)
- Performs comprehensive error checking with detailed error messages for debugging tape I/O issues
- The function reconstructs MinimalTuple format exactly as it was stored by `hashagg_spill_tuple`
- Memory for the returned tuple is allocated using `palloc` and must be freed by the caller
- The hash value is optional - pass NULL for `hashp` if the hash is not needed
- Tape reading follows the exact format written by the spilling functions: hash, length, tuple data