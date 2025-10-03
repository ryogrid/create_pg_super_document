# index_truncate_tuple

## Location
[src/backend/access/common/indextuple.c:576-608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/indextuple.c#L576-L608)

## Overview
Creates a palloc'd copy of an index tuple containing only the first specified number of attributes, effectively truncating the tuple while preserving its structure and essential data.

## Definition

```c
structure, so, plain pfree() should clean all allocated memory
	 */
	pfree(truncdesc);
```
## Detailed Description
The  function creates a truncated copy of an index tuple, keeping only the first  attributes while discarding the rest. This function is primarily used in B-tree operations where shorter tuple variants are needed for internal nodes or when space optimization is required.

The function performs several important operations:
1. Validates that the requested number of attributes doesn't exceed the source tuple's attributes
2. For the trivial case where no truncation is needed (leavenatts equals source attributes), it simply calls 
3. Creates a temporary tuple descriptor with the reduced attribute count
4. Uses  to extract values and null indicators from the source
5. Uses  to create a new tuple with only the specified attributes
6. Preserves the original tuple identifier (t_tid) in the truncated tuple
7. Cleans up the temporary descriptor to prevent memory leaks

The function is designed to be safe for use while holding buffer locks since it never performs external table access and doesn't handle EXTERNAL TOAST values.

## Parameters / Member Variables
- : TupleDesc describing the structure of the source tuple
- : The original IndexTuple to be truncated
- : Number of leading attributes to retain in the truncated tuple (must be ≤ sourceDescriptor->natts)

## Dependencies
- Functions called/Symbols referenced:
  - INDEX_MAX_KEYS
  - [CopyIndexTuple](../C/CopyIndexTuple.md)
  - TupleDescSize
  - [TupleDescCopy](../T/TupleDescCopy.md)
  - [index_deform_tuple](index_deform_tuple.md)
  - [index_form_tuple](index_form_tuple.md)
  - IndexTupleSize
  - [palloc](../p/palloc.md)
  - [pfree](../p/pfree.md)
  - Assert

- Called from (representative examples):
  - [_bt_truncate](../b/_bt_truncate.md)
  - IndexTupleHasVarwidths

## Notes and Other Information
- The function guarantees that the truncated tuple will be no larger than the original tuple
- It's safe to use the truncated tuple with the original tuple descriptor, but callers must avoid accessing truncated attributes
- Special care must be taken when using  with truncated tuples
- The function is designed to be called while holding buffer locks since it doesn't perform external operations
- Memory management is carefully handled to prevent leaks - the temporary descriptor is explicitly freed
- If PostgreSQL ever supports EXTERNAL TOAST values in index tuples, this function would need to be revisited
- The function preserves the tuple identifier (t_tid) from the source tuple
- Located in src/backend/access/common/indextuple.c at lines 576-608

## Simplified Source

```c
IndexTuple index_truncate_tuple(TupleDesc sourceDescriptor, IndexTuple source,
                               int leavenatts) {
    TupleDesc truncdesc;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    IndexTuple truncated;

    Assert(leavenatts <= sourceDescriptor->natts);

    // Easy case: no truncation needed
    if (leavenatts == sourceDescriptor->natts)
        return CopyIndexTuple(source);

    // Create temporary descriptor with reduced attribute count
    truncdesc = palloc(TupleDescSize(sourceDescriptor));
    TupleDescCopy(truncdesc, sourceDescriptor);
    truncdesc->natts = leavenatts;

    // Deform original tuple and form truncated copy
    index_deform_tuple(source, truncdesc, values, isnull);
    truncated = index_form_tuple(truncdesc, values, isnull);
    truncated->t_tid = source->t_tid;

    Assert(IndexTupleSize(truncated) <= IndexTupleSize(source));

    // Clean up temporary descriptor
    pfree(truncdesc);

    return truncated;
}
```