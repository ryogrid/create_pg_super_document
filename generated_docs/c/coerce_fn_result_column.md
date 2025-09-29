# coerce_fn_result_column

## Location
[src/backend/executor/functions.c:2003-2068](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L2003-L2068)

## Overview
Handles type coercion for individual columns in SQL function result sets, ensuring compatibility between actual and expected return types.

## Definition
```c
static bool coerce_fn_result_column(TargetEntry *src_tle, Oid res_type, int32 res_typmod,
                                   bool tlist_is_modifiable, List **upper_tlist,
                                   bool *upper_tlist_nontrivial)
```

## Detailed Description
coerce_fn_result_column is a specialized function that processes individual result columns for SQL function return type validation and coercion. It determines the optimal strategy for type coercion based on query constraints and either modifies the target list entry in-place or creates a new entry in an upper-level projection.

The function implements two distinct strategies:
1. **In-place modification**: When the target list is modifiable and the column has no sort/group references, it directly modifies the source target list entry
2. **Upper-level projection**: When in-place modification isn't safe, it creates a coercion expression in an upper target list

The function respects query semantics by avoiding modifications to columns referenced by ORDER BY, DISTINCT, or similar clauses that depend on specific column characteristics.

## Parameters / Member Variables
- `src_tle`: Source TargetEntry to be coerced
- `res_type`: OID of the required result type
- `res_typmod`: Type modifier for the required result type
- `tlist_is_modifiable`: Whether the source target list can be safely modified in-place
- `upper_tlist`: Pointer to list that will receive upper-level projection entries
- `upper_tlist_nontrivial`: Pointer to flag indicating if upper list contains non-trivial expressions

## Dependencies
- Functions called/Symbols referenced:
  - [coerce_to_target_type](coerce_to_target_type.md) (performs the actual type coercion)
  - [assign_expr_collations](../a/assign_expr_collations.md) (assigns collation information)
  - [makeVarFromTargetEntry](../m/makeVarFromTargetEntry.md) (creates Var nodes referencing target entries)
  - [makeTargetEntry](../m/makeTargetEntry.md) (creates new TargetEntry nodes)
  - [exprType](../e/exprType.md) (determines expression types)
- Called from (representative examples):
  - [check_sql_fn_retval](check_sql_fn_retval.md) (during return type validation)

## Notes and Other Information
- This is a static function, only accessible within functions.c
- Crucial for PostgreSQL's type safety in SQL function returns
- Implements intelligent coercion placement to maintain query semantics
- Handles both assignment-compatible coercions and implicit casts
- The function returns false if coercion is impossible, allowing calling code to handle the error appropriately
- Preserves column names and other metadata during coercion
- Works in conjunction with PostgreSQL's broader type coercion system
- Essential component of the SQL function return type checking infrastructure

## Simplified Source

```c
static bool
coerce_fn_result_column(TargetEntry *src_tle,
                        Oid res_type,
                        int32 res_typmod,
                        bool tlist_is_modifiable,
                        List **upper_tlist,
                        bool *upper_tlist_nontrivial)
{
    TargetEntry *new_tle;
    Expr *new_tle_expr;
    Node *cast_result;

    // Check if we can safely modify the source target entry in-place
    if (tlist_is_modifiable && src_tle->ressortgroupref == 0) {
        // Safe to modify in-place - coerce the source expression directly
        cast_result = coerce_to_target_type(NULL,
                                           (Node *) src_tle->expr,
                                           exprType((Node *) src_tle->expr),
                                           res_type, res_typmod,
                                           COERCION_ASSIGNMENT,
                                           COERCE_IMPLICIT_CAST,
                                           -1);
        if (cast_result == NULL)
            return false;  // Coercion failed

        assign_expr_collations(NULL, cast_result);
        src_tle->expr = (Expr *) cast_result;

        // Create a Var referencing the modified target entry
        new_tle_expr = (Expr *) makeVarFromTargetEntry(1, src_tle);
    } else {
        // Must perform coercion in upper target list to preserve semantics
        Var *var = makeVarFromTargetEntry(1, src_tle);

        cast_result = coerce_to_target_type(NULL,
                                           (Node *) var,
                                           var->vartype,
                                           res_type, res_typmod,
                                           COERCION_ASSIGNMENT,
                                           COERCE_IMPLICIT_CAST,
                                           -1);
        if (cast_result == NULL)
            return false;  // Coercion failed

        assign_expr_collations(NULL, cast_result);

        // Check if coercion actually changed anything
        if (cast_result != (Node *) var)
            *upper_tlist_nontrivial = true;

        new_tle_expr = (Expr *) cast_result;
    }

    // Create new target entry for the upper target list
    new_tle = makeTargetEntry(new_tle_expr,
                             list_length(*upper_tlist) + 1,
                             src_tle->resname, false);
    *upper_tlist = lappend(*upper_tlist, new_tle);

    return true;
}
```