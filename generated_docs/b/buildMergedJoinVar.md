# buildMergedJoinVar

## Location
src/backend/parser/parse_clause.c: 1666 - 1773

## Overview
Generates a suitable replacement expression for a merged join column, handling type coercion and join-type-specific logic for USING clause columns.

## Definition
```c
static Node *buildMergedJoinVar(ParseState *pstate, JoinType jointype,
                               Var *l_colvar, Var *r_colvar)
```

## Detailed Description
This function creates a unified expression for columns that appear in a JOIN USING clause, where the same-named columns from both sides of the join need to be merged into a single output column. It first determines the common output type and typmod using select_common_type and select_common_typmod, ensuring type compatibility between the left and right column variables. The function then applies necessary type coercions: if types differ, it uses coerce_type for explicit conversion; if only typmod differs, it applies makeRelabelType for implicit relabeling. The core logic varies by join type: for INNER joins, it prefers non-coerced variables when available; for LEFT joins, it always uses the left variable; for RIGHT joins, it always uses the right variable; for FULL OUTER joins, it constructs a COALESCE expression to handle null values from either side. Finally, it calls assign_expr_collations to ensure proper collation information is applied to any coercion or CoalesceExpr nodes created during the process.

## Parameters / Member Variables
- `pstate`: ParseState containing current parsing context for type resolution and coercion
- `jointype`: JoinType indicating the type of join (INNER, LEFT, RIGHT, FULL)
- `l_colvar`: Var representing the column from the left side of the join
- `r_colvar`: Var representing the column from the right side of the join

## Dependencies
- Functions called/Symbols referenced:
  - [select_common_type](../s/select_common_type.md)
  - [select_common_typmod](../s/select_common_typmod.md)
  - [coerce_type](../c/coerce_type.md)
  - [makeRelabelType](../m/makeRelabelType.md)
  - [assign_expr_collations](../a/assign_expr_collations.md)
  - list_make2
- Types referenced:
  - JoinType
  - Var
  - CoalesceExpr
- Constants referenced:
  - JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_FULL
  - COERCION_IMPLICIT, COERCE_IMPLICIT_CAST
- Called from (representative examples):
  - [transformFromClauseItem](../t/transformFromClauseItem.md) (for USING clause processing)

## Notes and Other Information
- This is a static function within parse_clause.c used internally for JOIN USING processing
- Critical for implementing SQL standard semantics for USING clause columns
- Handles type compatibility and coercion automatically between different column types
- For FULL OUTER joins, creates COALESCE expressions to properly handle NULL values
- The function ensures proper collation information is maintained through type coercions
- Essential for correct behavior of JOIN USING clauses with mixed column types
- Always applies assign_expr_collations to maintain proper collation semantics in the result