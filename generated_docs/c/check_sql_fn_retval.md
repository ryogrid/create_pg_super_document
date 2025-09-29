# check_sql_fn_retval

## Location
[src/backend/executor/functions.c:1609-2002](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L1609-L2002)

## Overview
Validates and potentially modifies the return value structure of SQL functions to ensure type compatibility between the function's declared return type and its actual output.

## Definition
```c
bool check_sql_fn_retval(List *queryTreeLists, Oid rettype, TupleDesc rettupdesc,
                        char prokind, bool insertDroppedCols, List **resultTargetList)
```

## Detailed Description
check_sql_fn_retval is a comprehensive function that performs critical type checking and coercion for SQL function return values. It analyzes the final statement in a SQL function's body to ensure the returned data matches the function's declared return type. The function can modify the query structure by injecting type coercions or even adding an extra Query level for projection when necessary.

Key responsibilities include:
1. **Finding the last canSetTag query** that determines the function's return value
2. **Type validation** for scalar, composite, and record return types  
3. **Type coercion** when compatible but not identical types are returned
4. **Tuple structure verification** for composite return types
5. **Query modification** by injecting coercion expressions or projection layers
6. **Handling dropped columns** in composite types when requested

The function returns a boolean indicating whether the result is a complete tuple (true) or just the first column (false).

## Parameters / Member Variables
- `queryTreeLists`: List of sublists containing Query nodes representing the function's parsed statements
- `rettype`: OID of the function's declared return type
- `rettupdesc`: Tuple descriptor for composite return types (can be NULL)
- `prokind`: Function kind (function vs procedure) affecting return handling
- `insertDroppedCols`: Whether to insert NULL columns for dropped attributes in composite types
- `resultTargetList`: Output parameter receiving the final target list (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecCleanTargetListLength](../E/ExecCleanTargetListLength.md) (counts non-junk target list entries)
  - [get_typtype](../g/get_typtype.md) (determines type category)
  - [coerce_fn_result_column](coerce_fn_result_column.md) (handles individual column coercion)
  - [makeConst](../m/makeConst.md), makeTargetEntry, makeAlias, makeFromExpr (AST construction)
  - [format_type_be](../f/format_type_be.md) (error message formatting)
- Called from (representative examples):
  - [fmgr_sql_validator](../f/fmgr_sql_validator.md) (during function creation/validation)
  - [init_sql_fcache](../i/init_sql_fcache.md) (during function cache initialization)
  - [inline_function](../i/inline_function.md), inline_set_returning_function (query optimization)

## Notes and Other Information
- Handles both scalar and composite return types with different validation strategies
- Can modify the original query structure by adding projection layers for type coercion
- Special handling for procedures vs functions, particularly regarding single composite returns
- Supports polymorphic types but requires actual resolved types for validation
- Integrates with PostgreSQL's type coercion system for maximum compatibility
- Critical for maintaining type safety in SQL function execution
- The function may inject NULL columns to handle dropped attributes in composite types
- Used extensively in both validation and optimization phases of query processing

## Simplified Source

```c
bool check_sql_fn_retval(List *queryTreeLists, Oid rettype, TupleDesc rettupdesc,
                        char prokind, bool insertDroppedCols, List **resultTargetList)
{
    bool is_tuple_result = false;
    Query *parse = NULL;
    List *tlist;
    int tlistlen;
    char fn_typtype;

    // Initialize result if provided
    if (resultTargetList)
        *resultTargetList = NIL;

    // Early return for VOID functions
    if (rettype == VOIDOID)
        return false;

    // Step 1: Find the last canSetTag query in function body
    foreach(lc, queryTreeLists) {
        List *sublist = lfirst_node(List, lc);
        foreach(lc2, sublist) {
            Query *q = lfirst_node(Query, lc2);
            if (q->canSetTag) {
                parse = q;  // This is our result-determining query
            }
        }
    }

    // Step 2: Extract target list from the final query
    if (parse && parse->commandType == CMD_SELECT) {
        tlist = parse->targetList;
    } else if (parse && (parse->commandType == CMD_INSERT ||
                        parse->commandType == CMD_UPDATE ||
                        parse->commandType == CMD_DELETE ||
                        parse->commandType == CMD_MERGE) &&
               parse->returningList) {
        tlist = parse->returningList;
    } else {
        // No valid result-producing statement
        ereport(ERROR, "Function's final statement must be SELECT or RETURNING");
        return false;
    }

    // Step 3: Validate return type compatibility
    tlistlen = ExecCleanTargetListLength(tlist);
    fn_typtype = get_typtype(rettype);

    if (fn_typtype == TYPTYPE_BASE || fn_typtype == TYPTYPE_DOMAIN ||
        fn_typtype == TYPTYPE_ENUM || fn_typtype == TYPTYPE_RANGE) {
        // Scalar return type: must have exactly one column
        if (tlistlen != 1) {
            ereport(ERROR, "Final statement must return exactly one column");
        }

        TargetEntry *tle = (TargetEntry *) linitial(tlist);
        if (!coerce_fn_result_column(tle, rettype, -1, true, &upper_tlist, &upper_tlist_nontrivial)) {
            ereport(ERROR, "Return type mismatch - cannot coerce result");
        }

    } else if (fn_typtype == TYPTYPE_COMPOSITE || rettype == RECORDOID) {
        // Composite return type: validate each column

        // Special case: single column that matches the composite type
        if (tlistlen == 1 && prokind != PROKIND_PROCEDURE) {
            TargetEntry *tle = (TargetEntry *) linitial(tlist);
            if (coerce_fn_result_column(tle, rettype, -1, true, &upper_tlist, &upper_tlist_nontrivial)) {
                goto validation_complete;  // Single composite column works
            }
        }

        // Multi-column validation against tuple descriptor
        if (rettupdesc) {
            int colindex = 0;
            foreach(lc, tlist) {
                TargetEntry *tle = (TargetEntry *) lfirst(lc);
                if (tle->resjunk) continue;  // Skip junk columns

                // Find next non-dropped attribute
                do {
                    colindex++;
                    if (colindex > rettupdesc->natts) {
                        ereport(ERROR, "Final statement returns too many columns");
                    }
                    attr = TupleDescAttr(rettupdesc, colindex - 1);

                    // Insert NULL for dropped columns if requested
                    if (attr->attisdropped && insertDroppedCols) {
                        insert_null_column_for_dropped_attribute();
                    }
                } while (attr->attisdropped);

                // Validate column type compatibility
                if (!coerce_fn_result_column(tle, attr->atttypid, attr->atttypmod,
                                           true, &upper_tlist, &upper_tlist_nontrivial)) {
                    ereport(ERROR, "Column type mismatch at position %d", colindex);
                }
            }

            // Check for missing columns (all remaining must be dropped)
            for (colindex++; colindex <= rettupdesc->natts; colindex++) {
                if (!TupleDescAttr(rettupdesc, colindex - 1)->attisdropped) {
                    ereport(ERROR, "Final statement returns too few columns");
                }
            }
        }

        is_tuple_result = true;  // Returning full tuple structure
    } else {
        ereport(ERROR, "Return type not supported for SQL functions");
    }

validation_complete:

    // Step 4: Create projection layer if type coercion was needed
    if (upper_tlist_nontrivial) {
        create_projection_query_layer(parse, upper_tlist);
    }

    // Return final target list if requested
    if (resultTargetList)
        *resultTargetList = upper_tlist;

    return is_tuple_result;
}
```