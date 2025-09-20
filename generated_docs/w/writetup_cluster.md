# writetup_cluster

## Location
[src/backend/utils/sort/tuplesortvariants.c:1355-1369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L1355-L1369)

## Overview
Writes a heap tuple to a logical tape during the CLUSTER sort operation, storing the tuple data along with its physical location information for later retrieval.

## Definition

```c
struct the HeapTupleData header */
	tuple->t_data = (HeapTupleHeader) ((char *) tuple + HEAPTUPLESIZE);
```
## Detailed Description
The  function is responsible for serializing heap tuples to logical tapes during CLUSTER sort operations. It writes the tuple in a format optimized for clustering, storing not only the tuple data but also the ItemPointer (t_self) which contains the physical location of the tuple in the heap. This information is crucial for the clustering operation as it needs to maintain the relationship between sorted tuples and their physical storage locations.

The function writes data in the following order:
1. Total length of the serialized data (for efficient reading)
2. The ItemPointer (t_self) containing the tuple's physical location
3. The actual tuple data (t_data)
4. Optionally, a trailing length word if random access is required

The serialization format is designed to be efficiently readable by the corresponding  function.

## Parameters / Member Variables
- : The tuplesort state containing configuration and context information
- : The logical tape to write the tuple data to
- : The SortTuple structure containing the heap tuple to be written

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - LogicalTapeWrite
  - TUPLESORT_RANDOMACCESS (flag)
- Called from (representative examples):
  - CLUSTER_SORT operations
  - [tuplesort_begin_cluster](../t/tuplesort_begin_cluster.md)

## Notes and Other Information
- This function is specific to CLUSTER operations and handles heap tuples differently from other tuple types
- The t_self field (ItemPointer) is preserved because clustering needs to maintain the relationship between sorted order and physical storage locations
- The trailing length word is written conditionally based on the TUPLESORT_RANDOMACCESS flag, which enables backward reading of the tape
- The function assumes the input tuple is a valid HeapTuple and does not perform validation
- Memory layout written: [length][t_self][tuple_data][optional_trailing_length]