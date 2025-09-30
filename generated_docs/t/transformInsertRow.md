# transformInsertRow

## Location
[src/backend/parser/analyze.c:1008-1117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L1008-L1117)

## Overview
Prepares a single INSERT row for assignment to the target table by transforming expressions and handling column assignments with optional indirection stripping.

## Definition
```c
List *
transformInsertRow(ParseState *pstate, List *exprlist,
                   List *stmtcols, List *icolumns, List *attrnos,
                   bool strip_indirection)
```

## Detailed Description
This function takes a list of expressions representing values for a single INSERT row and transforms them for assignment to target table columns. It performs several key operations:

1. **Length validation** - Ensures the expression list length is compatible with target columns
2. **Expression transformation** - Calls transformAssignedExpr for each value-column pair to handle type coercion, defaults, and indirection
3. **Indirection stripping** - Optionally removes field/array assignment nodes when processing multiple VALUES rows

The function handles various INSERT scenarios including direct VALUES, SELECT results, and expressions with field/array assignments. It provides detailed error messages for common mistakes like using parentheses to create RowExpr instead of separate columns.

Key design aspects:
- Validates expression count against target columns with helpful error messages  
- Supports partial column lists (remaining columns get defaults)
- Handles complex assignment expressions with indirection
- Can strip indirection nodes when building VALUES RTEs

## Parameters / Member Variables
- `pstate`: Parse state containing context and error position information
- `exprlist`: List of expressions to be assigned (from VALUES, SELECT, etc.)
- `stmtcols`: Original column specification from INSERT statement (used for error checking)
- `icolumns`: Effective target columns (list of ResTarget nodes)  
- `attrnos`: Integer attribute numbers corresponding to target columns
- `strip_indirection`: If true, removes FieldStore/SubscriptingRef nodes from expressions

## Dependencies
- Functions called/Symbols referenced:
  - [transformAssignedExpr](transformAssignedExpr.md) (main expression transformation with indirection handling)
  - [count_rowexpr_columns](../c/count_rowexpr_columns.md) (for detecting RowExpr usage in error cases)
  - [exprLocation](../e/exprLocation.md) (for error position reporting)
  - [list_nth](../l/list_nth.md) (for accessing list elements during validation)

- Called from (representative examples):
  - [transformInsertStmt](transformInsertStmt.md) (multiple times for different INSERT variants)
  - [transformMergeStmt](transformMergeStmt.md) (for MERGE statement processing)

## Notes and Other Information
- Critical for proper type coercion and default value handling in INSERT operations
- Provides enhanced error messages to help users identify common syntax mistakes
- The strip_indirection parameter is used when building VALUES RTEs where indirection must be applied later
- Handles complex assignment expressions including field updates and array element assignments
- Part of the larger INSERT statement transformation pipeline

## Simplified Source

```c
List *transformInsertRow(ParseState *pstate, List *exprlist,
                        List *stmtcols, List *icolumns, List *attrnos,
                        bool strip_indirection) {
    List *result;
    ListCell *lc, *icols, *attnos;

    // Validate expression count vs target columns
    if (list_length(exprlist) > list_length(icolumns))
        ereport(ERROR, "INSERT has more expressions than target columns");

    if (stmtcols != NIL && list_length(exprlist) < list_length(icolumns)) {
        // Special hint for common RowExpr mistake
        char *hint = NULL;
        if (list_length(exprlist) == 1 &&
            count_rowexpr_columns(pstate, linitial(exprlist)) == list_length(icolumns))
            hint = "Did you accidentally use extra parentheses?";

        ereport(ERROR, "INSERT has more target columns than expressions", hint);
    }

    // Transform each expression for assignment to target columns
    result = NIL;
    forthree(lc, exprlist, icols, icolumns, attnos, attrnos) {
        Expr *expr = (Expr *) lfirst(lc);
        ResTarget *col = lfirst_node(ResTarget, icols);
        int attno = lfirst_int(attnos);

        // Transform expression for assignment (handles type coercion, indirection)
        expr = transformAssignedExpr(pstate, expr,
                                    EXPR_KIND_INSERT_TARGET,
                                    col->name, attno,
                                    col->indirection, col->location);

        // Strip indirection if requested (for VALUES RTE building)
        if (strip_indirection) {
            while (expr) {
                Expr *subexpr = expr;

                // Skip over CoerceToDomain nodes
                while (IsA(subexpr, CoerceToDomain))
                    subexpr = ((CoerceToDomain *) subexpr)->arg;

                // Handle FieldStore and SubscriptingRef nodes
                if (IsA(subexpr, FieldStore)) {
                    expr = (Expr *) linitial(((FieldStore *) subexpr)->newvals);
                } else if (IsA(subexpr, SubscriptingRef)) {
                    SubscriptingRef *sbsref = (SubscriptingRef *) subexpr;
                    if (sbsref->refassgnexpr == NULL)
                        break;
                    expr = sbsref->refassgnexpr;
                } else {
                    break;
                }
            }
        }

        result = lappend(result, expr);
    }

    return result;
}
```

**Key Points:**
- Prepares INSERT row expressions for assignment to target table columns
- Validates expression count against target columns with helpful error messages
- Detects common mistake of using extra parentheses (creating RowExpr)
- Transforms each expression with type coercion and indirection handling
- Optionally strips FieldStore/SubscriptingRef nodes when building VALUES RTEs
- Returns list of transformed expressions ready for execution