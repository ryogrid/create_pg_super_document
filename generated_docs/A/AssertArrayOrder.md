# AssertArrayOrder

## Location
[src/backend/access/brin/brin_minmax_multi.c:279-295](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L279-L295)

## Overview
AssertArrayOrder is a static debugging function that validates the sorted order of an array of values using a comparison function, ensuring data integrity in BRIN minmax-multi indexes.

## Definition

```c
static void
AssertArrayOrder(FmgrInfo *cmp, Oid colloid, Datum *values, int nvalues)
```
## Detailed Description
This function performs assertion-based validation of array element ordering in debug builds. It iterates through an array of Datum values and verifies that each consecutive pair is in the correct sorted order using the provided comparison function. The function is specifically designed for BRIN (Block Range Index) minmax-multi access method to ensure that value arrays maintain their sorted invariant, which is critical for the proper functioning of range-based indexing operations.

The function uses BTLessStrategyNumber comparison semantics, meaning it expects the comparison function to return true when the first argument is less than the second argument. Any violation of this ordering triggers an assertion failure, helping developers catch data corruption or sorting bugs during development.

## Parameters / Member Variables
- `*cmp`: FmgrInfo pointer to the comparison function that implements BTLessStrategyNumber semantics
- `colloid`: OID of the collation to use for comparison operations
- `*values`: Array of Datum values to validate for correct ordering
- `nvalues`: Number of elements in the values array
## Dependencies
- Functions called/Symbols referenced:
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
- Called from (representative examples):
  - [AssertCheckRanges](AssertCheckRanges.md)

## Notes and Other Information
- This is a debug-only function that only executes when assertions are enabled
- The function assumes the comparison function follows PostgreSQL's BTLessStrategyNumber protocol
- Part of the BRIN minmax-multi access method implementation in src/backend/access/brin/brin_minmax_multi.c
- Critical for maintaining data integrity in block range indexes that store multiple values per range

## Simplified Source

```c
static void
AssertArrayOrder(FmgrInfo *cmp, Oid colloid, Datum *values, int nvalues)
{
    // Verify each pair of consecutive values is in sorted order
    for (int i = 0; i < (nvalues - 1); i++)
    {
        Datum lt = FunctionCall2Coll(cmp, colloid, values[i], values[i + 1]);
        Assert(DatumGetBool(lt));  // values[i] < values[i+1]
    }
}
```