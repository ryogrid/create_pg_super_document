# TemporalSimplify

## Location
[src/backend/utils/adt/datetime.c:4840-4872](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L4840-L4872)

## Overview
TemporalSimplify optimizes temporal type length-coercion function calls by simplifying or eliminating unnecessary precision conversions when the target precision is equal to or less restrictive than the source precision.

## Definition

```c
Node *
TemporalSimplify(int32 max_precis, Node *node)
```
## Detailed Description
This function serves as common optimization code for temporal prosupport functions (time, timetz, timestamp, timestamptz). It analyzes function calls that perform precision coercion on temporal types and determines if the coercion can be simplified or eliminated entirely.

The function examines the source and target precisions:
- If the target precision is unspecified (< 0) or equals the maximum precision, the coercion is unnecessary
- If the source already has adequate precision (old_precis >= new_precis), the coercion can be replaced with a simple RelabelType node
- This optimization reduces runtime overhead by eliminating redundant function calls

The function operates on FuncExpr nodes representing calls to temporal coercion functions like timestamp_scale.

## Parameters / Member Variables
- `max_precis`: The maximum precision allowed for the temporal type (e.g., 6 for microseconds)
- `*node`: A FuncExpr node representing the temporal coercion function call to be analyzed
## Dependencies
- Functions called/Symbols referenced:
  - castNode (macro for safe casting)
  - [list_length](../l/list_length.md) (list utility function)
  - lsecond (get second list element)
  - IsA (type checking macro)
  - linitial (get first list element)  
  - [exprTypmod](../e/exprTypmod.md) (extract type modifier from expression)
  - [DatumGetInt32](../D/DatumGetInt32.md) (extract int32 from Datum)
  - [relabel_to_typmod](../r/relabel_to_typmod.md) (create RelabelType node)
- Called from (representative examples):
  - [time_support](../t/time_support.md) (src/backend/utils/adt/date.c:1614)
  - [timestamp_support](../t/timestamp_support.md) (src/backend/utils/adt/timestamp.c:334)

## Notes and Other Information
- This is part of PostgreSQL's planner support functions that help optimize queries at parse/plan time
- The optimization is safe because temporal precision coercion is monotonic - higher precision values can always be safely treated as lower precision
- The function assumes typmod validation has already occurred during parsing (typmodin functions)
- Returns NULL if no optimization is possible, allowing the original function call to proceed

## Simplified Source

```c
Node *TemporalSimplify(int32 max_precis, Node *node) {
    FuncExpr *expr = castNode(FuncExpr, node);
    Node *ret = NULL;

    // Function must have at least 2 arguments (value and typmod)
    Assert(list_length(expr->args) >= 2);

    // Get the precision typmod (second argument)
    Node *typmod = (Node *)lsecond(expr->args);

    // Only optimize if typmod is a constant
    if (IsA(typmod, Const) && !((Const *)typmod)->constisnull) {
        Node *source = (Node *)linitial(expr->args);
        int32 old_precis = exprTypmod(source);
        int32 new_precis = DatumGetInt32(((Const *)typmod)->constvalue);

        // Can optimize if:
        // - New precision is unspecified (< 0)
        // - New precision equals maximum precision
        // - Source already has adequate precision (old >= new)
        if (new_precis < 0 || new_precis == max_precis ||
            (old_precis >= 0 && new_precis >= old_precis)) {
            // Replace with simple relabel - no function call needed
            ret = relabel_to_typmod(source, new_precis);
        }
    }

    return ret;  // NULL if no optimization possible
}
```