# free_conversion_map

## Location
[src/backend/access/common/tupconvert.c:299-308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupconvert.c#L299-L308)

## Overview
Deallocates a TupleConversionMap structure and all its associated memory resources, properly cleaning up conversion state used for tuple format transformation.

## Definition


## Detailed Description
This function performs complete memory deallocation of a TupleConversionMap structure, which is used in PostgreSQL to convert tuples between different formats or schemas. The function systematically frees all dynamically allocated components of the conversion map, including the attribute mapping, value arrays, and null indicator arrays. Importantly, the function does not free the input and output tuple descriptors (indesc and outdesc) as these are managed elsewhere and may be shared across multiple conversion maps.

The deallocation follows a careful order to ensure all memory is properly released without leaks, using PostgreSQL's memory management functions.

## Parameters / Member Variables
- : Pointer to the TupleConversionMap structure to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [free_attrmap](free_attrmap.md) (frees the attribute mapping component)
  - [pfree](../p/pfree.md) (PostgreSQL's memory deallocation function, used multiple times)
  - TupleConversionMap (the structure type being deallocated)
- Called from (representative examples):
  - [acquire_inherited_sample_rows](../a/acquire_inherited_sample_rows.md) (src/backend/commands/analyze.c:1562)
  - [tstoreShutdownReceiver](../t/tstoreShutdownReceiver.md) (src/backend/executor/tstoreReceiver.c:218)

## Notes and Other Information
- The indesc and outdesc tuple descriptors are explicitly NOT freed by this function as they are managed externally
- Uses pfree() for memory deallocation, which is PostgreSQL's standard memory management function
- Called during cleanup operations when tuple conversion is no longer needed
- Part of the tuple conversion subsystem that handles schema mapping and data format transformation
- The function assumes all pointers within the TupleConversionMap are valid; calling with a partially initialized or corrupted structure may cause issues