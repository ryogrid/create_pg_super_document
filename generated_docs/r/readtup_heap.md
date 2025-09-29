# readtup_heap

## Location
[src/backend/utils/sort/tuplestore.c:1521-1535](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L1521-L1535)

## Overview
Reads a minimal tuple from a logical tape and reconstructs it into a SortTuple structure for heap sort operations during external sorting.

## Definition
```c
static void readtup_heap(Tuplesortstate *state, SortTuple *stup, LogicalTape *tape, unsigned int len)
```

## Detailed Description
This function is the counterpart to writetup_heap, responsible for deserializing heap tuples from logical tapes during external sorting operations. It reads tuple data from the specified tape, reconstructs the minimal tuple structure, and sets up the SortTuple for further processing. The function also extracts the first column key value from the tuple for sorting purposes.

The function operates by:
1. Calculating the tuple body length from the provided total length
2. Allocating memory for the minimal tuple using tuplesort_readtup_alloc
3. Reading the tuple body data from the logical tape
4. Optionally reading trailing length information for random access support
5. Setting up the SortTuple structure with the reconstructed tuple
6. Extracting the first column value for sorting comparison using heap_getattr

## Parameters / Member Variables
- `state`: The tuplesort state containing sorting configuration and context information
- `stup`: The SortTuple structure to populate with the read tuple data
- `tape`: The logical tape to read the tuple data from
- `len`: The total length of the tuple data to read (including length header)

## Dependencies
- Functions called/Symbols referenced:
  - [tuplesort_readtup_alloc](../t/tuplesort_readtup_alloc.md)
  - TuplesortstateGetPublic
  - LogicalTapeReadExact
  - [heap_getattr](../h/heap_getattr.md)
- Data structures used:
  - TuplesortPublic
  - MinimalTuple
  - [LogicalTape](../L/LogicalTape.md)
  - SortTuple
  - [HeapTupleData](../H/HeapTupleData.md)
  - HeapTupleHeader
- Constants referenced:
  - MINIMAL_TUPLE_DATA_OFFSET
  - MINIMAL_TUPLE_OFFSET
  - TUPLESORT_RANDOMACCESS
- Called from (representative examples):
  - [tuplesort_begin_heap](../t/tuplesort_begin_heap.md)
  - [tuplestore_begin_heap](../t/tuplestore_begin_heap.md)

## Notes and Other Information
- This is a static function specific to heap tuple sorting operations
- The function reads tuple data in the format written by the corresponding writetup_heap function
- Random access support reads trailing length information when TUPLESORT_RANDOMACCESS option is enabled
- The function reconstructs both the minimal tuple structure and extracts the first sorting key for comparison
- Uses heap_getattr to extract the first column value, which is cached in the SortTuple for efficient sorting
- Part of PostgreSQL's external sorting infrastructure, handling the deserialization phase of spilled tuples
- The reconstructed HeapTupleData is used temporarily to extract attribute values but is not stored permanently

## Simplified Source

```c
static void readtup_heap(Tuplesortstate *state, SortTuple *stup,
                         LogicalTape *tape, unsigned int len)
{
    TuplesortPublic *base = TuplesortstateGetPublic(state);

    // Calculate tuple sizes
    unsigned int tupbodylen = len - sizeof(int);
    unsigned int tuplen = tupbodylen + MINIMAL_TUPLE_DATA_OFFSET;

    // Allocate memory for the minimal tuple
    MinimalTuple tuple = (MinimalTuple) tuplesort_readtup_alloc(state, tuplen);
    char *tupbody = (char *) tuple + MINIMAL_TUPLE_DATA_OFFSET;

    // Read tuple data from tape
    tuple->t_len = tuplen;
    LogicalTapeReadExact(tape, tupbody, tupbodylen);

    // Read trailing length for random access support if needed
    if (base->sortopt & TUPLESORT_RANDOMACCESS)
        LogicalTapeReadExact(tape, &tuplen, sizeof(tuplen));

    // Set up the SortTuple
    stup->tuple = (void *) tuple;

    // Extract first column value for sorting comparisons
    HeapTupleData htup;
    htup.t_len = tuple->t_len + MINIMAL_TUPLE_OFFSET;
    htup.t_data = (HeapTupleHeader) ((char *) tuple - MINIMAL_TUPLE_OFFSET);

    stup->datum1 = heap_getattr(&htup,
                                base->sortKeys[0].ssup_attno,
                                (TupleDesc) base->arg,
                                &stup->isnull1);
}
```