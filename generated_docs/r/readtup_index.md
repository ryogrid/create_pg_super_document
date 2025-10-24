# readtup_index

## Location
[src/backend/utils/sort/tuplesortvariants.c:1687-1710](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L1687-L1710)

## Overview
A specialized function for reading and deserializing index tuples from logical tapes during external sorting, reconstructing SortTuple structures with proper key extraction.

## Definition

```c
static void
readtup_index(Tuplesortstate *state, SortTuple *stup,
			  LogicalTape *tape, unsigned int len)
```
## Detailed Description
This function deserializes an IndexTuple from a logical tape during the external sorting merge phase. It performs the inverse operation of writetup_index:

1. **Memory allocation**: Allocates memory for the tuple using tuplesort_readtup_alloc with the calculated tuple length
2. **Tuple deserialization**: Reads the tuple data from the tape using LogicalTapeReadExact
3. **Random access handling**: If random access is enabled, reads and discards the trailing length word
4. **SortTuple reconstruction**: Populates the SortTuple structure and extracts the first column key value for comparison optimization

The extracted first-column key (datum1) and its null flag (isnull1) are cached in the SortTuple to avoid repeated attribute extraction during comparisons.

## Parameters / Member Variables
- `*state`: Tuplesortstate containing sorting configuration and context
- `*stup`: SortTuple structure to populate with the deserialized tuple
- `*tape`: LogicalTape to read the tuple data from
- `len`: Total length of the serialized data (including length prefix)
## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - [tuplesort_readtup_alloc](../t/tuplesort_readtup_alloc.md)
  - LogicalTapeReadExact
  - [index_getattr](../i/index_getattr.md)
  - RelationGetDescr
  - TUPLESORT_RANDOMACCESS (flag constant)
- Called from (representative examples):
  - [tuplesort_begin_index_btree](../t/tuplesort_begin_index_btree.md)
  - [tuplesort_begin_index_hash](../t/tuplesort_begin_index_hash.md)
  - [tuplesort_begin_index_gist](../t/tuplesort_begin_index_gist.md)
  - CLUSTER_SORT

## Notes and Other Information
- The function calculates the actual tuple length by subtracting the length field size from the total length
- Memory allocation is handled by tuplesort_readtup_alloc, which manages memory contexts appropriately
- The first column key extraction optimizes subsequent comparison operations by avoiding repeated index_getattr calls
- Must handle the trailing length word when TUPLESORT_RANDOMACCESS is enabled to maintain tape position
- This function is the counterpart to writetup_index and must handle the same serialization format
- LogicalTapeReadExact ensures complete reads and handles I/O errors appropriately
- The function assumes the IndexTuple format with the first column being the primary sort key

## Simplified Source

```c
static void
readtup_index(Tuplesortstate *state, SortTuple *stup,
              LogicalTape *tape, unsigned int len)
{
    TuplesortPublic *base = TuplesortstateGetPublic(state);
    TuplesortIndexArg *arg = (TuplesortIndexArg *) base->arg;

    // Calculate actual tuple length and allocate memory
    unsigned int tuplen = len - sizeof(unsigned int);
    IndexTuple tuple = (IndexTuple) tuplesort_readtup_alloc(state, tuplen);

    // Read tuple data from tape
    LogicalTapeReadExact(tape, tuple, tuplen);

    // Skip trailing length word if random access is enabled
    if (base->sortopt & TUPLESORT_RANDOMACCESS)
        LogicalTapeReadExact(tape, &tuplen, sizeof(tuplen));

    // Populate SortTuple and extract first column key for optimization
    stup->tuple = (void *) tuple;
    stup->datum1 = index_getattr(tuple, 1,
                                 RelationGetDescr(arg->indexRel),
                                 &stup->isnull1);
}
```