# isCurrentGroup

## Location
[src/backend/executor/nodeIncrementalSort.c:212-285](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIncrementalSort.c#L212-L285)

## Overview
A static function that determines whether a given tuple belongs to the current sort group by comparing the pre-sorted column values with a pivot tuple.

## Definition

```c
static bool
isCurrentGroup(IncrementalSortState *node, TupleTableSlot *pivot, TupleTableSlot *tuple)
```
## Detailed Description
This function is a critical component of the incremental sort algorithm that determines group boundaries. It compares the pre-sorted column values between a pivot tuple (representing the current group) and a new tuple to determine if they belong to the same group. The function performs equality comparisons on all pre-sorted columns using the cached comparison functions set up by preparePresortedCols.

The function implements an optimization by comparing columns in reverse order (from last to first pre-sorted column) because in sorted input, trailing keys are more likely to change first, allowing for early detection of inequality and minimizing the number of function calls. The function handles NULL values specially, treating NULL-vs-NULL as equal and NULL-vs-non-NULL as unequal.

## Parameters / Member Variables
- : Pointer to IncrementalSortState containing cached comparison functions and metadata
- : TupleTableSlot representing the current group (used as comparison baseline)
- : TupleTableSlot to test for membership in the current group

## Dependencies
- Functions called/Symbols referenced:
  - castNode (macro to safely cast plan node)
  - [slot_getattr](../s/slot_getattr.md) (extracts attribute value from tuple slot)
  - FunctionCallInvoke (invokes cached comparison function)
  - [DatumGetBool](../D/DatumGetBool.md) (extracts boolean result from Datum)
  - [IncrementalSort](../I/IncrementalSort.md) (plan node type)
  - [PresortedKeyData](../P/PresortedKeyData.md) (structure containing cached comparison functions)
- Called from (representative examples):
  - [switchToPresortedPrefixMode](../s/switchToPresortedPrefixMode.md) (when switching sort modes)
  - [ExecIncrementalSort](../E/ExecIncrementalSort.md) (main execution function, multiple locations)

## Notes and Other Information
- The function returns true if the tuple belongs to the current group, false otherwise
- Optimization: compares pre-sorted columns in reverse order for better performance with sorted input
- NULL handling: treats NULL == NULL as true, NULL != non-NULL as false
- Uses pre-cached comparison functions from preparePresortedCols for efficiency
- Essential for determining when to finalize the current sort group and start a new one
- Error checking ensures comparison functions don't return unexpected NULL results

## Simplified Source

```c
static bool isCurrentGroup(IncrementalSortState *node, TupleTableSlot *pivot, TupleTableSlot *tuple)
{
    int nPresortedCols = castNode(IncrementalSort, node->ss.ps.plan)->nPresortedCols;

    // Compare presorted columns in reverse order for optimization
    // (tail keys more likely to change in sorted input)
    for (int i = nPresortedCols - 1; i >= 0; i--)
    {
        Datum datumA, datumB, result;
        bool isnullA, isnullB;
        AttrNumber attno = node->presorted_keys[i].attno;
        PresortedKeyData *key;

        // Extract attribute values from both tuples
        datumA = slot_getattr(pivot, attno, &isnullA);
        datumB = slot_getattr(tuple, attno, &isnullB);

        // Handle NULL values: NULL == NULL is true, NULL != non-NULL is false
        if (isnullA || isnullB)
        {
            if (isnullA == isnullB)
                continue;  // Both NULL, equal
            else
                return false;  // One NULL, one not - different group
        }

        // Use cached comparison function to test equality
        key = &node->presorted_keys[i];
        key->fcinfo->args[0].value = datumA;
        key->fcinfo->args[1].value = datumB;
        key->fcinfo->isnull = false;

        result = FunctionCallInvoke(key->fcinfo);

        // Ensure comparison function didn't return NULL
        if (key->fcinfo->isnull)
            elog(ERROR, "function %u returned NULL", key->flinfo.fn_oid);

        // If values are not equal, tuple belongs to different group
        if (!DatumGetBool(result))
            return false;
    }

    return true;  // All presorted columns match - same group
}
```