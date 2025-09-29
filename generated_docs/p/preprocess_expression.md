# preprocess_expression

## Location
[src/backend/optimizer/plan/planner.c:1156-1257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L1156-L1257)

## Overview
The preprocess_expression function performs comprehensive preprocessing of SQL expressions, including join alias flattening, constant simplification, qualification canonicalization, SubLink expansion, and correlation variable replacement.

## Definition
```c
static Node *preprocess_expression(PlannerInfo *root, Node *expr, int kind)
```

## Detailed Description
The preprocess_expression function implements a systematic pipeline for transforming SQL expressions into their optimized, canonical forms suitable for further planning operations. It handles various types of expressions including target lists, WHERE clauses, JOIN/ON conditions, HAVING clauses, and other query components.

The preprocessing pipeline follows a specific sequence:
1. Early exit optimization for NULL expressions
2. Join alias variable flattening (expands aliases to base relation variables)
3. Constant expression evaluation and simplification
4. Qualification canonicalization for WHERE/HAVING clauses
5. ScalarArrayOpExpr optimization with hash lookup conversion
6. SubLink expansion to SubPlan structures
7. Correlation variable replacement with parameter references
8. Implicit-AND format conversion for qualification expressions

The function is context-aware, applying different optimizations based on the expression kind (EXPRKIND_TARGET, EXPRKIND_QUAL, EXPRKIND_RTFUNC, etc.). Special handling prevents unnecessary processing for certain RTE types like non-lateral functions, VALUES lists, and TABLESAMPLE clauses.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and state information
- `expr`: Node pointer to the expression tree to be preprocessed (can be NULL)
- `kind`: Integer constant specifying the expression context type (EXPRKIND_TARGET, EXPRKIND_QUAL, EXPRKIND_RTFUNC, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [flatten_join_alias_vars](../f/flatten_join_alias_vars.md) (alias expansion)
  - [eval_const_expressions](../e/eval_const_expressions.md) (constant folding)
  - [canonicalize_qual](../c/canonicalize_qual.md) (qualification normalization)
  - [convert_saop_to_hashed_saop](../c/convert_saop_to_hashed_saop.md) (array operation optimization)
  - [SS_process_sublinks](../S/SS_process_sublinks.md) (SubLink to SubPlan transformation)
  - [SS_replace_correlation_vars](../S/SS_replace_correlation_vars.md) (parameter substitution)
  - [make_ands_implicit](../m/make_ands_implicit.md) (AND format conversion)
- Called from (representative examples):
  - [subquery_planner](../s/subquery_planner.md) (extensive usage across all expression types)
  - [preprocess_qual_conditions](preprocess_qual_conditions.md) (recursive qualification processing)
  - standard_qp_extra (additional expression handling)

## Notes and Other Information
- Critical for converting named-argument function calls to positional notation and inserting default argument values
- Flattens nested AND/OR expressions into N-argument form for uniform processing
- Maintains careful ordering to ensure SubLinks expanded from join aliases are properly processed
- Includes optimizer debug support with conditional pretty-printing
- Essential effect includes constant expression evaluation which cannot be skipped in modern PostgreSQL versions
- Handles correlation variables by replacing them with Param nodes for outer query references
- Located in src/backend/optimizer/plan/planner.c:1156-1257

## Simplified Source
```c
static Node *preprocess_expression(PlannerInfo *root, Node *expr, int kind)
{
    // Quick exit for empty expressions
    if (expr == NULL)
        return NULL;

    // Step 1: Flatten join alias variables to base-relation variables
    // Skip for certain RTE types that can't contain query-level Vars
    if (root->hasJoinRTEs &&
        !(kind == EXPRKIND_RTFUNC ||
          kind == EXPRKIND_VALUES ||
          kind == EXPRKIND_TABLESAMPLE ||
          kind == EXPRKIND_TABLEFUNC))
        expr = flatten_join_alias_vars(root, root->parse, expr);

    // Step 2: Simplify constant expressions and convert function calls
    // This also flattens nested AND/OR expressions
    if (kind != EXPRKIND_RTFUNC)
        expr = eval_const_expressions(root, expr);

    // Step 3: Canonicalize qualification expressions
    if (kind == EXPRKIND_QUAL) {
        expr = (Node *) canonicalize_qual((Expr *) expr, false);
    }

    // Step 4: Optimize ScalarArrayOpExpr with hash lookups
    if (kind == EXPRKIND_QUAL || kind == EXPRKIND_TARGET) {
        convert_saop_to_hashed_saop(expr);
    }

    // Step 5: Expand SubLinks to SubPlans
    if (root->parse->hasSubLinks)
        expr = SS_process_sublinks(root, expr, (kind == EXPRKIND_QUAL));

    // Step 6: Replace correlation variables with Param nodes
    if (root->query_level > 1)
        expr = SS_replace_correlation_vars(root, expr);

    // Step 7: Convert qual expressions to implicit-AND format
    if (kind == EXPRKIND_QUAL)
        expr = (Node *) make_ands_implicit((Expr *) expr);

    return expr;
}
```