# pg_get_expr

## Location
[src/backend/utils/adt/ruleutils.c:2629-2645](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L2629-L2645)

## Overview
This function decompiles stored expression trees back into human-readable SQL expressions, supporting expressions that reference a single relation or are relation-independent.

## Definition

```c
Datum
pg_get_expr(PG_FUNCTION_ARGS)
```
## Detailed Description
pg_get_expr is a PostgreSQL built-in function that converts stored expression trees (in nodeToString format) back into readable SQL text. It serves as the primary interface for decompiling expressions that are stored in various system catalog columns as pg_node_tree data. The function is designed to handle expressions that reference at most one relation, making it suitable for partial index expressions, column default values, check constraints, and other single-relation expressions.

The function provides a safe interface that gracefully handles cases where the referenced relation may no longer exist, returning NULL instead of throwing errors. This behavior is particularly useful when examining catalog entries for recently dropped relations. The function uses indented pretty-printing by default to make complex expressions more readable.

## Parameters / Member Variables
-  (text): The expression tree in nodeToString format to be decompiled
-  (Oid): The object identifier of the relation that the expression references, or InvalidOid for relation-independent expressions

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (macro for extracting text argument with possible detoasting)
  - PG_GETARG_OID (macro for extracting OID argument)
  - PRETTYFLAG_INDENT (constant for indented formatting)
  - [pg_get_expr_worker](pg_get_expr_worker.md) (core worker function that performs the actual decompilation)
  - PG_RETURN_TEXT_P (macro for returning text result)
  - PG_RETURN_NULL (macro for returning NULL result)
- Called from (representative examples):
  - [decompile_conbin](../d/decompile_conbin.md) (in table command processing)
  - Various SQL queries that need to display stored expressions
  - System information functions and views

## Notes and Other Information
- This function is exposed to SQL users as pg_get_expr(pg_node_tree, oid)
- Limited to expressions referencing a single relation or no relation at all
- Cannot handle complex query trees like those in pg_rewrite.ev_action
- Returns NULL for invalid relation OIDs rather than throwing errors for robustness
- Uses indented pretty-printing to make complex expressions more readable
- Essential for displaying human-readable versions of stored constraint expressions, default values, and partial index predicates
- Part of PostgreSQL's expression decompilation system used throughout the database
- Located in src/backend/utils/adt/ruleutils.c:2629-2645
- The actual decompilation work is delegated to pg_get_expr_worker function

## Simplified Source

```c
// Simplified version of pg_get_expr
Datum pg_get_expr(PG_FUNCTION_ARGS) {
    // Extract input parameters: expression text and relation OID
    text *expr = PG_GETARG_TEXT_PP(0);
    Oid relid = PG_GETARG_OID(1);
    text *result;

    // Set formatting flags for indented output
    int prettyFlags = PRETTYFLAG_INDENT;

    // Delegate actual decompilation to worker function
    result = pg_get_expr_worker(expr, relid, prettyFlags);

    // Return result or NULL if decompilation failed
    if (result)
        PG_RETURN_TEXT_P(result);
    else
        PG_RETURN_NULL();
}
```

Key simplifications made:
- Removed detailed comments while preserving essential functionality understanding
- Simplified variable declarations and initialization
- Added high-level comments explaining the core logic flow
- Focused on the main execution path: extract parameters → set flags → call worker → return result