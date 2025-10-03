# TidRangeEval

## Location
[src/backend/executor/nodeTidrangescan.c:137-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTidrangescan.c#L137-L219)

## Overview
TidRangeEval computes and sets the block and offset range to scan by evaluating TID expressions, determining the minimum and maximum ItemPointer bounds for the scan.

## Definition

```c
static bool
TidRangeEval(TidRangeScanState *node)
```
## Detailed Description
This function evaluates all TID comparison expressions stored in the TidRangeScanState to determine the actual range of ItemPointers (TIDs) that need to be scanned. It starts with the absolute limits of the ItemPointer type and progressively narrows the range based on each TidOpExpr. The function handles both inclusive and exclusive bounds by normalizing non-inclusive ranges. It processes both lower and upper bounds, updating the scan range only when a more restrictive bound is found. The final computed range is stored in the node's trss_mintid and trss_maxtid fields.

## Parameters / Member Variables
- `node`: TidRangeScanState containing the TID expressions to evaluate and storage for the computed range bounds

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerSet](../I/ItemPointerSet.md)
  - lfirst
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - [ExecEvalExprSwitchContext](../E/ExecEvalExprSwitchContext.md)
  - [ItemPointerCopy](../I/ItemPointerCopy.md)
  - [ItemPointerInc](../I/ItemPointerInc.md)
  - [ItemPointerDec](../I/ItemPointerDec.md)
  - [ItemPointerCompare](../I/ItemPointerCompare.md)
- Constants used:
  - InvalidBlockNumber
  - PG_UINT16_MAX
  - TIDEXPR_LOWER_BOUND
  - TIDEXPR_UPPER_BOUND
- Data structures used:
  - [ItemPointerData](../I/ItemPointerData.md)
  - [TidOpExpr](TidOpExpr.md)
- Called from:
  - [TidRangeNext](TidRangeNext.md)

## Notes and Other Information
- Returns false if any TID expression evaluates to NULL, indicating no tuples can match
- Returns true if the range is valid and may contain tuples
- Non-inclusive bounds are normalized by incrementing lower bounds and decrementing upper bounds
- The function only narrows the range - it never expands it beyond the current bounds
- Initial bounds span the entire possible ItemPointer range from (0,0) to (InvalidBlockNumber, PG_UINT16_MAX)
- The resulting ItemPointer values may not represent valid tuple locations due to normalization

## Simplified Source

```c
static bool
TidRangeEval(TidRangeScanState *node)
{
    ExprContext *econtext = node->ss.ps.ps_ExprContext;
    ItemPointerData lowerBound, upperBound;
    ListCell   *l;

    // Initialize bounds to absolute limits
    ItemPointerSet(&lowerBound, 0, 0);
    ItemPointerSet(&upperBound, InvalidBlockNumber, PG_UINT16_MAX);

    // Process each TID comparison expression
    foreach(l, node->trss_tidexprs)
    {
        TidOpExpr  *tidopexpr = (TidOpExpr *) lfirst(l);
        ItemPointer itemptr;
        bool isNull;

        // Evaluate the TID expression
        itemptr = (ItemPointer) DatumGetPointer(
            ExecEvalExprSwitchContext(tidopexpr->exprstate, econtext, &isNull));

        // NULL bound means no matches possible
        if (isNull)
            return false;

        if (tidopexpr->exprtype == TIDEXPR_LOWER_BOUND)
        {
            ItemPointerData lb;
            ItemPointerCopy(itemptr, &lb);

            // Make non-inclusive bounds inclusive
            if (!tidopexpr->inclusive)
                ItemPointerInc(&lb);

            // Narrow the lower bound if this is more restrictive
            if (ItemPointerCompare(&lb, &lowerBound) > 0)
                ItemPointerCopy(&lb, &lowerBound);
        }
        else if (tidopexpr->exprtype == TIDEXPR_UPPER_BOUND)
        {
            ItemPointerData ub;
            ItemPointerCopy(itemptr, &ub);

            // Make non-inclusive bounds inclusive
            if (!tidopexpr->inclusive)
                ItemPointerDec(&ub);

            // Narrow the upper bound if this is more restrictive
            if (ItemPointerCompare(&ub, &upperBound) < 0)
                ItemPointerCopy(&ub, &upperBound);
        }
    }

    // Store the computed range in the scan state
    ItemPointerCopy(&lowerBound, &node->trss_mintid);
    ItemPointerCopy(&upperBound, &node->trss_maxtid);

    return true;
}
```