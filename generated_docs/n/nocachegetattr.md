# nocachegetattr

## Location
[src/backend/access/common/heaptuple.c:519-722](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L519-L722)

## Overview
nocachegetattr extracts attribute values from heap tuples when cached offsets cannot be used, implementing an optimization strategy that caches computed attribute offsets in the tuple descriptor for future use.

## Definition


## Detailed Description
nocachegetattr is a performance-critical function called from fastgetattr() when cached offsets are not available and the requested attribute is not null. It handles the complex task of locating attribute data within a tuple while dealing with variable-length attributes, null values, and alignment requirements.

The function implements a sophisticated caching strategy that stores computed offsets in the tuple descriptor's attribute metadata (attcacheoff field). This allows subsequent accesses to the same attributes in other tuples using the same tuple descriptor to skip the expensive offset calculation.

The function handles three main scenarios:
1. No nulls and no variable-width attributes - fastest path with simple offset calculation
2. Has nulls or variable-width attributes after the target attribute - can still use some optimizations
3. Has nulls or variable-width attributes before the target attribute - requires careful traversal

Key optimizations include:
- Checking for nulls in preceding attributes using bitwise operations
- Bulk initialization of cached offsets for all leading fixed-width columns
- Strategic caching decisions based on alignment requirements for variable-length attributes

## Parameters / Member Variables
- : HeapTuple containing the tuple data to extract from
- : Attribute number to extract (1-based indexing)
- : TupleDesc describing the tuple structure and containing cached offset information

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleNoNulls, HeapTupleHasVarWidth, HeapTupleHasNulls (tuple property checks)
  - TupleDescAttr (access tuple descriptor attributes)
  - att_isnull (check null bitmap)
  - fetchatt (extract final attribute value)
  - att_align_nominal, att_align_pointer (handle data alignment)
  - att_addlength_pointer (calculate variable-length attribute sizes)
- Called from (representative examples):
  - [fastgetattr](../f/fastgetattr.md) (primary caller - inline macro)
  - HeapTupleClearHeapOnly

## Notes and Other Information
- Only called from fastgetattr() when cached offsets are unavailable and the value is not null
- Implements a crucial performance optimization by caching attribute offsets in the tuple descriptor
- The caching strategy significantly improves performance for queries processing many tuples with the same structure
- Must handle complex alignment requirements for different data types
- Coordinates with heap_deform_tuple and nocache_index_getattr which use similar logic
- The offset caching is conservative for variable-length attributes - only caches when alignment is guaranteed
- Uses bit manipulation for efficient null checking in the tuple's null bitmap
- Critical for PostgreSQL's tuple access performance, especially for wide tables with many attributes
- The function converts from 1-based attribute numbering (external interface) to 0-based internal indexing
- Balances between computation cost and cache effectiveness to optimize overall system performance