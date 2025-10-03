# heap_attr_equals

## Location
[src/backend/access/heap/heapam.c:4303-4353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L4303-L4353)

## Overview
heap_attr_equals is a static helper function that compares two attribute values for equality, specifically designed as a subroutine for HeapDetermineColumnsInfo to determine if column values have changed.

## Definition

```c
static bool
heap_attr_equals(TupleDesc tupdesc, int attrnum, Datum value1, Datum value2,
				 bool isnull1, bool isnull2)
```
## Detailed Description
This function performs attribute value comparison with careful handling of NULL values and different data types. It implements a conservative binary comparison approach rather than using type-specific equality operators for safety reasons. The function is designed to avoid false positives while operating under exclusive buffer locks, where invoking user-defined functions would be unsafe.

The comparison logic follows these steps:
1. First checks if NULL status differs between the two values
2. If both values are NULL, considers them equal
3. For system columns (attrnum <= 0), performs OID comparison
4. For regular columns, uses binary comparison via datumIsEqual

## Parameters / Member Variables
- `tupdesc`: Tuple descriptor containing attribute metadata
- `attrnum`: Attribute number (1-based for regular columns, <= 0 for system columns)
- `value1`: First datum value to compare
- `value2`: Second datum value to compare
- `isnull1`: NULL indicator for first value
- `isnull2`: NULL indicator for second value
## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetObjectId](../D/DatumGetObjectId.md) (for system column comparison)
  - [datumIsEqual](../d/datumIsEqual.md) (for binary datum comparison)
  - TupleDescAttr (macro to access attribute descriptor)
- Called from (representative examples):
  - [HeapDetermineColumnsInfo](../H/HeapDetermineColumnsInfo.md)

## Notes and Other Information
- Uses conservative binary comparison to avoid false positives
- Cannot safely invoke user-defined equality functions while holding exclusive buffer locks
- Handles system columns (OIDs) separately from regular table columns
- May be overly strict as multiple binary representations can exist for the same logical value
- Part of PostgreSQL's heap access method implementation for determining column changes

## Simplified Source

```c
static bool heap_attr_equals(TupleDesc tupdesc, int attrnum, Datum value1, Datum value2,
                            bool isnull1, bool isnull2)
{
    Form_pg_attribute att;

    // Handle NULL value comparisons first
    if (isnull1 != isnull2)
        return false;  // One NULL, one not NULL

    if (isnull1)
        return true;   // Both are NULL

    // Compare actual values based on column type
    if (attrnum <= 0)
    {
        // System columns: treat as OIDs
        return (DatumGetObjectId(value1) == DatumGetObjectId(value2));
    }
    else
    {
        // Regular columns: use binary comparison
        Assert(attrnum <= tupdesc->natts);
        att = TupleDescAttr(tupdesc, attrnum - 1);
        return datumIsEqual(value1, value2, att->attbyval, att->attlen);
    }
}
```