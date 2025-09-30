# ExecIndexEvalArrayKeys

## Location
[src/backend/executor/nodeIndexscan.c:661-739](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexscan.c#L661-L739)

## Overview
Evaluates array key expressions, decomposes arrays into individual elements, and initializes scankeys for array-based index operations.

## Definition

```c
struct the array expression. (Notes in
		 * ExecIndexEvalRuntimeKeys() apply here too.)
		 */
		arraydatum = ExecEvalExpr(array_expr,
								  econtext,
								  &isNull);
```
## Detailed Description
The `ExecIndexEvalArrayKeys` function handles the evaluation and setup of array-based index scan keys. It evaluates array expressions, decomposes them into individual elements, and prepares the scan infrastructure to iterate through array elements during index scans. This is essential for queries using array operators like `ANY` or `ALL` with index scans.

The function evaluates each array expression in the provided context, validates that arrays are non-null and non-empty, and decomposes them into individual elements using PostgreSQL's array deconstruction utilities. It initializes the scan keys with the first element of each array and sets up the iteration state for subsequent calls to `ExecIndexAdvanceArrayKeys`.

If any array is null or empty, the function returns false, indicating that no matches are possible since array operations require at least one element to compare against.

## Parameters
- `econtext`: The expression context containing current execution state and variable values for expression evaluation
- `arrayKeys`: Array of IndexArrayKeyInfo structures containing array expressions and associated scan keys
- `numArrayKeys`: The number of array keys in the arrayKeys array

## Return Value
- `true`: All arrays have been successfully evaluated and contain at least one element; scankeys are initialized with first elements
- `false`: At least one array is null or empty, meaning no matches are possible

## Dependencies
- Functions called/Symbols referenced:
  - [ExecEvalExpr](ExecEvalExpr.md)
  - DatumGetArrayTypeP
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md)
  - [deconstruct_array](../d/deconstruct_array.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Data types used:
  - [IndexArrayKeyInfo](../I/IndexArrayKeyInfo.md)
  - ScanKey
  - [ExprContext](ExprContext.md)
  - [ArrayType](../A/ArrayType.md)
- Constants/Macros used:
  - ARR_ELEMTYPE
  - SK_ISNULL

## Called From
- [ExecReScanBitmapIndexScan](ExecReScanBitmapIndexScan.md) (src/backend/executor/nodeBitmapIndexscan.c:157)

## Notes and Other Information
- Allocates array element data in per-tuple memory context for proper lifecycle management
- Automatically handles memory cleanup through context reset, avoiding explicit pfree calls
- Sets up iteration state by storing decomposed array elements and initializing next_elem counter
- Handles null elements within arrays by setting SK_ISNULL flag appropriately
- Critical for implementing array comparison operators (ANY, ALL) in index scans
- Must be followed by calls to ExecIndexAdvanceArrayKeys to iterate through remaining array elements
- Early termination on first null/empty array optimizes performance by avoiding unnecessary work

## Simplified Source

```c
bool ExecIndexEvalArrayKeys(ExprContext *econtext,
                           IndexArrayKeyInfo *arrayKeys, int numArrayKeys) {
    bool result = true;

    // Switch to per-tuple memory context for array storage
    MemoryContext oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    // Process each array key
    for (int j = 0; j < numArrayKeys; j++) {
        ScanKey scan_key = arrayKeys[j].scan_key;
        ExprState *array_expr = arrayKeys[j].array_expr;

        // Evaluate the array expression
        bool isNull;
        Datum arraydatum = ExecEvalExpr(array_expr, econtext, &isNull);

        if (isNull) {
            result = false;
            break;  // No point continuing if any array is null
        }

        // Deconstruct the array into individual elements
        ArrayType *arrayval = DatumGetArrayTypeP(arraydatum);
        int16 elmlen;
        bool elmbyval;
        char elmalign;

        get_typlenbyvalalign(ARR_ELEMTYPE(arrayval), &elmlen, &elmbyval, &elmalign);

        Datum *elem_values;
        bool *elem_nulls;
        int num_elems;

        deconstruct_array(arrayval, ARR_ELEMTYPE(arrayval),
                         elmlen, elmbyval, elmalign,
                         &elem_values, &elem_nulls, &num_elems);

        if (num_elems <= 0) {
            result = false;
            break;  // Empty array means no matches possible
        }

        // Store array elements and initialize scan key with first element
        arrayKeys[j].elem_values = elem_values;
        arrayKeys[j].elem_nulls = elem_nulls;
        arrayKeys[j].num_elems = num_elems;
        arrayKeys[j].next_elem = 1;

        // Set up scan key with first element
        scan_key->sk_argument = elem_values[0];
        if (elem_nulls[0])
            scan_key->sk_flags |= SK_ISNULL;
        else
            scan_key->sk_flags &= ~SK_ISNULL;
    }

    MemoryContextSwitchTo(oldContext);
    return result;
}
```