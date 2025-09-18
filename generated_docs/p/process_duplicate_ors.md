# process_duplicate_ors

## Location
src/backend/optimizer/prep/prepqual.c: 517 - 676

## Overview
Applies the inverse OR distributive law to a list of OR-connected expressions by factoring out common sub-expressions that appear in all OR branches.

## Definition
```c
static Expr *process_duplicate_ors(List *orlist)
```

## Detailed Description
This function implements a query optimization technique that transforms OR expressions by applying the inverse distributive law. Given an OR expression like `(A AND B) OR (A AND C)`, it can be factored to `A AND (B OR C)`, potentially reducing the computational cost of evaluating the expression.

The function works by:
1. Finding the shortest AND clause in the OR list to use as a reference (non-AND expressions are treated as single-element AND clauses)
2. Identifying which sub-expressions appear in ALL branches of the OR
3. Factoring out these common expressions
4. Constructing a new expression in the form `common_factors AND (remaining_or_expression)`

Special cases handled:
- Empty OR list returns FALSE
- Single expression OR returns the expression itself
- If any clause becomes empty after factoring, the entire expression reduces to just the common factors (degenerate case)

The optimization is particularly effective for expressions like:
- `(A AND B) OR (A AND C)` → `A AND (B OR C)`
- `(A AND B AND C) OR (A AND D)` → `A AND ((B AND C) OR D)`

## Parameters / Member Variables
- `orlist`: List of expressions that are connected by OR operations to be processed for common factor extraction

## Dependencies
- Functions called/Symbols referenced:
  - [makeBoolConst](../m/makeBoolConst.md)
  - [is_andclause](../i/is_andclause.md)
  - BoolExpr
  - list_union
  - [list_member](../l/list_member.md)
  - [equal](../e/equal.md)
  - [make_orclause](../m/make_orclause.md)
  - [list_difference](../l/list_difference.md)
  - [make_andclause](../m/make_andclause.md)
  - [pull_ors](pull_ors.md)
  - [pull_ands](pull_ands.md)
- Called from (representative examples):
  - [find_duplicate_ors](../f/find_duplicate_ors.md)

## Notes and Other Information
- This is a static function located in src/backend/optimizer/prep/prepqual.c:517-676
- Part of PostgreSQL's query preprocessing and optimization system
- The function maintains AND/OR expression flatness to prevent nested structures that could complicate further optimization
- Uses list operations extensively to manipulate expression trees
- Returns the optimized expression which could be an AND clause, OR clause, or even a single sub-expression depending on the input
- The algorithm is conservative - if no common factors are found, it returns the original OR clause unchanged