# index_form_tuple

## Location
[src/backend/access/common/indextuple.c:44-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/indextuple.c#L44-L64)

## Overview
The `index_form_tuple` function creates an IndexTuple from arrays of attribute values and null indicators, allocating the tuple in the current memory context.

## Definition
```c
IndexTuple index_form_tuple(TupleDesc tupleDescriptor, const Datum *values, const bool *isnull)
```

## Detailed Description
This function serves as a convenience wrapper around `index_form_tuple_context`. It constructs an IndexTuple by combining attribute values and null indicators according to the provided tuple descriptor. The key difference from its underlying function is that it automatically uses the CurrentMemoryContext for memory allocation, making it suitable for most common use cases where the caller doesn't need to specify a particular memory context.

The function is part of PostgreSQL's index tuple interface routines and provides a simplified API for creating index tuples without requiring explicit memory context management.

## Parameters
- `tupleDescriptor`: TupleDesc that describes the structure and attributes of the tuple to be formed
- `values`: Array of Datum values for each attribute in the tuple
- `isnull`: Array of boolean flags indicating which attributes are NULL

## Dependencies
- Functions called/Symbols referenced:
  - [index_form_tuple_context](index_form_tuple_context.md)
  - CurrentMemoryContext (global variable)
- Called from (representative examples):
  - [index_truncate_tuple](index_truncate_tuple.md) (src/backend/access/common/indextuple.c:597)
  - [GinFormTuple](../G/GinFormTuple.md) (src/backend/access/gin/ginentrypage.c:68)
  - gistFormTuple (src/backend/access/gist/gistutil.c:582)
  - [hashbuildCallback](../h/hashbuildCallback.md) (src/backend/access/hash/hash.c:234)
  - [hashinsert](../h/hashinsert.md) (src/backend/access/hash/hash.c:268)
  - [btinsert](../b/btinsert.md) (src/backend/access/nbtree/nbtree.c:192)

## Notes and Other Information
- This is a thin wrapper function that delegates all actual work to `index_form_tuple_context`
- Located in src/backend/access/common/indextuple.c:44-64
- Used extensively across different index access methods (B-tree, Hash, GiST, GIN)
- The function simplifies memory management by automatically using the current memory context