# writetup_index

## Location
src/backend/utils/sort/tuplesortvariants.c: 1673 - 1686

## Overview
A specialized function for writing index tuples to logical tapes during external sorting, handling tuple serialization with proper length prefixes and optional random access support.

## Definition


## Detailed Description
This function serializes an IndexTuple to a logical tape during the external sorting process. It implements the standard tuple serialization format used by PostgreSQL's tuplesort infrastructure:

1. **Length prefix**: Writes the total length (tuple size + length field size) as a header
2. **Tuple data**: Writes the actual IndexTuple data
3. **Trailing length** (conditional): If random access is enabled (TUPLESORT_RANDOMACCESS), writes the length again at the end to support backward scanning

The function is essential for external sorting when memory is insufficient to hold all tuples, allowing sorted runs to be written to temporary storage and later merged.

## Parameters / Member Variables
- : Tuplesortstate containing sorting configuration and context
- : LogicalTape to write the tuple data to
- : SortTuple containing the IndexTuple to serialize

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - IndexTupleSize
  - LogicalTapeWrite
  - TUPLESORT_RANDOMACCESS (flag constant)
- Called from (representative examples):
  - [tuplesort_begin_index_btree](../t/tuplesort_begin_index_btree.md)
  - [tuplesort_begin_index_hash](../t/tuplesort_begin_index_hash.md)
  - [tuplesort_begin_index_gist](../t/tuplesort_begin_index_gist.md)
  - CLUSTER_SORT

## Notes and Other Information
- The length field includes both the tuple size and the size of the length field itself for proper deserialization
- Random access support adds a trailing length word to enable backward scanning through the tape
- This function is part of the external sorting infrastructure and is only called when memory is insufficient for in-memory sorting
- The serialization format must be compatible with the corresponding readtup_index function
- [LogicalTape](../L/LogicalTape.md) provides an abstraction over temporary file I/O with buffering and compression support
- [IndexTuple](../I/IndexTuple.md) is a specialized tuple format used for index entries, distinct from heap tuples