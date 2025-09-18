# preprocess_expression

## Location
src/backend/optimizer/plan/planner.c: 1156 - 1257

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
  - flatten_join_alias_vars (alias expansion)
  - eval_const_expressions (constant folding)
  - canonicalize_qual (qualification normalization)
  - convert_saop_to_hashed_saop (array operation optimization)
  - SS_process_sublinks (SubLink to SubPlan transformation)
  - SS_replace_correlation_vars (parameter substitution)
  - make_ands_implicit (AND format conversion)
- Called from (representative examples):
  - subquery_planner (extensive usage across all expression types)
  - preprocess_qual_conditions (recursive qualification processing)
  - standard_qp_extra (additional expression handling)

## Notes and Other Information
- Critical for converting named-argument function calls to positional notation and inserting default argument values
- Flattens nested AND/OR expressions into N-argument form for uniform processing
- Maintains careful ordering to ensure SubLinks expanded from join aliases are properly processed
- Includes optimizer debug support with conditional pretty-printing
- Essential effect includes constant expression evaluation which cannot be skipped in modern PostgreSQL versions
- Handles correlation variables by replacing them with Param nodes for outer query references
- Located in src/backend/optimizer/plan/planner.c:1156-1257