# get_rule_expr_toplevel

## Location
src/backend/utils/adt/ruleutils.c: 10325 - 10342

## Overview
A specialized wrapper around get_rule_expr() that handles top-level expressions with special treatment for whole-row variables.

## Definition

```c
static void
get_rule_expr_toplevel(Node *node, deparse_context *context,
					   bool showimplicit)
```
## Detailed Description
 is a thin wrapper function that provides special handling for expressions appearing at the top level of certain SQL contexts. The key difference from  is its treatment of Var nodes representing whole-row variables.

When the input node is a Var (variable reference), this function calls  with the  parameter set to true. This causes whole-row variables to be printed with special decoration that prevents parser expansion of "*" syntax. This behavior is crucial in contexts like ROW() expressions and VALUES() clauses where the parser would otherwise expand "foo.*" at the top level.

For all other node types, it simply delegates to the standard  function.

## Parameters / Member Variables
- : The parse tree node to convert to SQL text (can be NULL)
- : Deparse context containing output buffer, formatting options, and namespace information
- : Boolean flag controlling whether implicit casts are displayed in the output

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - get_variable
  - get_rule_expr

- Called from (representative examples):
  - get_values_def
  - get_rule_expr (for RowExpr handling)
  - get_rule_list_toplevel

## Notes and Other Information
- Prevents unwanted "*" expansion in contexts where whole-row references need explicit handling
- Essential for proper deparsing of ROW() expressions and VALUES() clauses
- Maintains semantic correctness when reparsing top-level variable references
- Part of PostgreSQL's rule system infrastructure for accurate SQL reconstruction