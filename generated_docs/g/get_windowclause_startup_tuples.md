# get_windowclause_startup_tuples

## Location
[src/backend/optimizer/path/costsize.c:2854-3067](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L2854-L3067)

## Overview
Estimates how many tuples a WindowAgg node needs to fetch from its subnode before it can output the first tuple, based on the window clause specifications including partitioning, ordering, and frame options.

## Definition
static double get_windowclause_startup_tuples(PlannerInfo *root, WindowClause *wc, double input_tuples)

## Detailed Description
The get_windowclause_startup_tuples function analyzes a WindowClause to determine how many input tuples must be read before a WindowAgg node can produce its first output tuple. This depends heavily on the window specification:

- **No PARTITION BY, no ORDER BY**: All input tuples must be read and aggregated before any output
- **With PARTITION BY**: Only tuples from the first partition need to be considered
- **Frame specifications**: Different frame options (ROWS, RANGE, GROUPS) with various bounds (UNBOUNDED, CURRENT ROW, PRECEDING, FOLLOWING) affect how many tuples are needed

The function performs a multi-step analysis:
1. Estimates partition size by calculating the number of partitions using estimate_num_groups
2. Estimates peer group size within partitions based on ORDER BY expressions
3. Analyzes frame options to determine the specific number of tuples required
4. Handles various frame ending conditions including offset calculations for numeric constants

For OFFSET FOLLOWING frames, the function attempts to extract exact values from Const nodes (INT2, INT4, INT8) or falls back to selectivity-based estimates using DEFAULT_INEQ_SEL when the offset is not a constant.

## Parameters / Member Variables
- : PlannerInfo structure containing parse tree and planner context
-       0       0       0: WindowClause containing partition, order, and frame specifications
- : Total number of input tuples from the subnode

## Dependencies
- Functions called/Symbols referenced:
  - [get_sortgrouplist_exprs](get_sortgrouplist_exprs.md) (extracts expressions from sort/group lists)
  - [estimate_num_groups](../e/estimate_num_groups.md) (estimates distinct groups in expressions)
  - [list_free](../l/list_free.md) (memory management for expression lists)
  - [clamp_row_est](../c/clamp_row_est.md) (ensures row estimates are within reasonable bounds)
  - [DatumGetInt16](../D/DatumGetInt16.md), DatumGetInt32, DatumGetInt64 (extract values from Datum)
  - Various FRAMEOPTION constants (END_UNBOUNDED_FOLLOWING, END_CURRENT_ROW, etc.)
  - DEFAULT_INEQ_SEL (default selectivity for inequality conditions)
- Called from (representative examples):
  - [cost_windowagg](../c/cost_windowagg.md) (in costsize.c:3146)

## Notes and Other Information
- Function is static, indicating internal use within costsize.c only
- Adds +1 tuple when partitioning/ordering is present to account for WindowAgg needing to read ahead to confirm partition/group boundaries
- EXCLUDE options in window frames don't affect tuple reading count, only aggregation
- Handles unsupported frame options gracefully with assertions and fallback to 1.0
- For NULL constants in OFFSET clauses, assumes only first row/range/group is needed
- Return value is capped to never exceed the estimated partition size
- Uses DEFAULT_INEQ_SEL heuristic when offset values cannot be determined from non-constant expressions
- Considers peer groups (tuples with identical ORDER BY values) for RANGE and GROUPS frame modes

## Simplified Source

```c
static double get_windowclause_startup_tuples(PlannerInfo *root, WindowClause *wc, double input_tuples) {
    int frameOptions = wc->frameOptions;
    double partition_tuples;
    double peer_tuples;
    double return_tuples;

    // Calculate partition size
    if (wc->partitionClause != NIL) {
        List *partexprs = get_sortgrouplist_exprs(wc->partitionClause, root->parse->targetList);
        double num_partitions = estimate_num_groups(root, partexprs, input_tuples, NULL, NULL);
        list_free(partexprs);
        partition_tuples = input_tuples / num_partitions;
    } else {
        partition_tuples = input_tuples;
    }

    // Calculate peer group size
    if (wc->orderClause != NIL) {
        List *orderexprs = get_sortgrouplist_exprs(wc->orderClause, root->parse->targetList);
        double num_groups = estimate_num_groups(root, orderexprs, partition_tuples, NULL, NULL);
        list_free(orderexprs);
        peer_tuples = partition_tuples / num_groups;
    } else {
        peer_tuples = 1.0;
    }

    // Determine tuples needed based on frame options
    if (frameOptions & FRAMEOPTION_END_UNBOUNDED_FOLLOWING) {
        return_tuples = partition_tuples;
    } else if (frameOptions & FRAMEOPTION_END_CURRENT_ROW) {
        if (frameOptions & FRAMEOPTION_ROWS) {
            return_tuples = 1.0;
        } else if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) {
            return_tuples = (wc->orderClause == NIL) ? partition_tuples : peer_tuples;
        } else {
            return_tuples = 1.0;
        }
    } else if (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING) {
        return_tuples = 1.0;
    } else if (frameOptions & FRAMEOPTION_END_OFFSET_FOLLOWING) {
        // Extract offset value for FOLLOWING frames
        Const *endOffset = (Const *) wc->endOffset;
        double end_offset_value;

        if (IsA(endOffset, Const) && !endOffset->constisnull) {
            // Extract numeric offset based on type
            switch (endOffset->consttype) {
                case INT2OID: end_offset_value = (double) DatumGetInt16(endOffset->constvalue); break;
                case INT4OID: end_offset_value = (double) DatumGetInt32(endOffset->constvalue); break;
                case INT8OID: end_offset_value = (double) DatumGetInt64(endOffset->constvalue); break;
                default: end_offset_value = partition_tuples / peer_tuples * DEFAULT_INEQ_SEL; break;
            }
        } else {
            end_offset_value = partition_tuples / peer_tuples * DEFAULT_INEQ_SEL;
        }

        if (frameOptions & FRAMEOPTION_ROWS) {
            return_tuples = end_offset_value + 1.0;
        } else {
            return_tuples = peer_tuples * (end_offset_value + 1.0);
        }
    } else {
        return_tuples = 1.0;
    }

    // Add extra tuple for boundary detection if needed
    if (wc->partitionClause != NIL || wc->orderClause != NIL) {
        return_tuples = Min(return_tuples + 1.0, partition_tuples);
    } else {
        return_tuples = Min(return_tuples, partition_tuples);
    }

    return clamp_row_est(return_tuples);
}
```