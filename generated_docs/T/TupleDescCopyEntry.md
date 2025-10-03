# TupleDescCopyEntry

## Location
[src/backend/access/common/tupdesc.c:289-330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L289-L330)

## Overview
Copies a single attribute structure from one tuple descriptor to another without copying constraints or defaults.

## Definition

```c
void
TupleDescCopyEntry(TupleDesc dst, AttrNumber dstAttno,
				   TupleDesc src, AttrNumber srcAttno)
```
## Detailed Description
This function copies a single attribute definition from a source tuple descriptor to a destination tuple descriptor at specified positions. It performs a memory copy of the fixed-part attribute structure, updates the attribute number, and resets the cache offset. Like other copy functions in this family, it explicitly does not copy constraint-related information and clears all constraint flags in the destination attribute. The function includes sanity checks to ensure valid source and destination descriptors and attribute numbers.

## Parameters / Member Variables
- `dst`: Destination TupleDesc to copy the attribute into
- `dstAttno`: Attribute number in destination (1-based index)
- `src`: Source TupleDesc to copy the attribute from
- `srcAttno`: Attribute number in source (1-based index)
## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid
  - ATTRIBUTE_FIXED_PART_SIZE
- Called from (representative examples):
  - [ExecInitFunctionScan](../E/ExecInitFunctionScan.md)
  - [addRangeTableEntryForFunction](../a/addRangeTableEntryForFunction.md)
  - [ordered_set_startup](../o/ordered_set_startup.md)

## Notes and Other Information
- Performs attribute-level copying rather than full descriptor copying
- Includes sanity checks with Assert statements for parameter validation
- Updates destination attribute number and resets attcacheoff to -1
- Clears constraint-related flags (attnotnull, atthasdef, atthasmissing, attidentity, attgenerated)
- Optimized to avoid O(N^2) penalty by not resetting cache offsets of following columns
- Used primarily in scenarios where individual attributes need to be copied between different tuple descriptors

## Simplified Source

```c
void
TupleDescCopyEntry(TupleDesc dst, AttrNumber dstAttno,
                   TupleDesc src, AttrNumber srcAttno)
{
    Form_pg_attribute dstAtt = TupleDescAttr(dst, dstAttno - 1);
    Form_pg_attribute srcAtt = TupleDescAttr(src, srcAttno - 1);

    // Basic parameter validation
    Assert(PointerIsValid(src) && PointerIsValid(dst));
    Assert(srcAttno >= 1 && srcAttno <= src->natts);
    Assert(dstAttno >= 1 && dstAttno <= dst->natts);

    // Copy the fixed-size attribute structure
    memcpy(dstAtt, srcAtt, ATTRIBUTE_FIXED_PART_SIZE);

    // Update destination-specific fields
    dstAtt->attnum = dstAttno;
    dstAtt->attcacheoff = -1;  // Reset cache offset

    // Clear constraint-related flags (constraints not copied)
    dstAtt->attnotnull = false;
    dstAtt->atthasdef = false;
    dstAtt->atthasmissing = false;
    dstAtt->attidentity = '\0';
    dstAtt->attgenerated = '\0';
}
```