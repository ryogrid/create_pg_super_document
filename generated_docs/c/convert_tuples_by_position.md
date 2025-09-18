# convert_tuples_by_position

## Location
[src/backend/access/common/tupconvert.c:59-101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupconvert.c#L59-L101)

## Overview
Creates a tuple conversion map by matching input and output columns by their physical positions, ignoring dropped columns in both descriptors.

## Definition


## Detailed Description
This function sets up tuple conversion infrastructure when tuples need to be converted between different tuple descriptors where the correspondence is based on column position rather than column names. It first verifies logical compatibility between input and output descriptors using , then creates and initializes a  structure with preallocated workspace arrays for efficient tuple conversion operations.

The function returns  if no runtime conversion is needed (descriptors are physically compatible), otherwise returns a fully initialized conversion map that can be used with  or .

## Parameters
- : Input tuple descriptor defining the source tuple structure
- : Output tuple descriptor defining the target tuple structure  
- : Error message to use if compatibility check fails (should be prepared with gettext_noop())

## Dependencies
- Functions called/Symbols referenced:
  - [build_attrmap_by_position](../b/build_attrmap_by_position.md)
  - TupleConversionMap (struct)
  - [AttrMap](../A/AttrMap.md) (struct)
  - [palloc](../p/palloc.md)
- Called from (representative examples):
  - [tstoreStartupReceiver](../t/tstoreStartupReceiver.md)

## Notes and Other Information
- Dropped columns are ignored in both input and output descriptors during position-based matching
- The returned map contains preallocated workspace arrays (outvalues, outisnull, invalues, inisnull) for efficient conversion
- The map references the provided tuple descriptors, so they must remain valid for the map's lifetime
- Memory allocation occurs in the caller's memory context
- Position 0 in the invalues/inisnull arrays is reserved for NULL values