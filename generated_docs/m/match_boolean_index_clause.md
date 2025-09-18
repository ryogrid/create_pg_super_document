# match_boolean_index_clause

## Location
src/backend/optimizer/path/indxpath.c: 2305 - 2391

## Overview
Recognizes restriction clauses that can be matched to a boolean index by transforming boolean expressions into indexable equality operations.

## Definition
```c
static IndexClause *
match_boolean_index_clause(PlannerInfo *root,
                           RestrictInfo *rinfo,
                           int indexcol,
                           IndexOptInfo *index)
```

## Detailed Description
This function handles the special case of boolean indexes by transforming various boolean expressions into indexable equality clauses using the `BooleanEqualOperator`. It addresses a critical need introduced since PostgreSQL 8.1, where constant simplification performs reverse transformations that would otherwise prevent boolean indexes from being used.

The function recognizes and transforms four types of boolean expressions:

1. **Direct reference**: `indexkey` → `indexkey = TRUE`
2. **NOT clause**: `NOT indexkey` → `indexkey = FALSE`  
3. **IS TRUE test**: `indexkey IS TRUE` → `indexkey = TRUE`
4. **IS FALSE test**: `indexkey IS FALSE` → `indexkey = FALSE`

All transformations create explicit equality operations that can be processed by the index's equality operator, enabling efficient boolean index scans.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context
- `rinfo`: RestrictInfo node containing the boolean clause to be analyzed
- `indexcol`: Zero-based column number of the boolean index column
- `index`: IndexOptInfo structure with metadata about the target index

## Dependencies
- Functions called/Symbols referenced:
  - [match_index_to_operand](match_index_to_operand.md)
  - make_opclause
  - [makeBoolConst](makeBoolConst.md)
  - [is_notclause](../i/is_notclause.md)
  - [get_notclausearg](../g/get_notclausearg.md)
  - make_simple_restrictinfo
  - makeNode (IndexClause creation)
- Constants used:
  - BooleanEqualOperator
  - IS_TRUE
  - IS_FALSE
- Called from (representative examples):
  - ec_member_matches_arg
  - [match_clause_to_indexcol](match_clause_to_indexcol.md)
  - [indexcol_is_bool_constant_for_query](../i/indexcol_is_bool_constant_for_query.md)

## Notes and Other Information
- Should only be called when `IsBooleanOpfamily()` confirms boolean operator family support
- Transforms are only applied at the top level of WHERE clauses
- IS TRUE/IS FALSE handling ignores NULL semantics differences since they occur at top level
- All generated IndexClause nodes are marked as non-lossy since transformations are exact
- Essential for boolean index utilization in modern PostgreSQL versions
- Located in `src/backend/optimizer/path/indxpath.c:2305-2391`