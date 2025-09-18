# check_sql_fn_retval

## Location
src/backend/executor/functions.c: 1609 - 2002

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
  - ExecCleanTargetListLength (counts non-junk target list entries)
  - get_typtype (determines type category)
  - coerce_fn_result_column (handles individual column coercion)
  - makeConst, makeTargetEntry, makeAlias, makeFromExpr (AST construction)
  - format_type_be (error message formatting)
- Called from (representative examples):
  - fmgr_sql_validator (during function creation/validation)
  - init_sql_fcache (during function cache initialization)
  - inline_function, inline_set_returning_function (query optimization)

## Notes and Other Information
- Handles both scalar and composite return types with different validation strategies
- Can modify the original query structure by adding projection layers for type coercion
- Special handling for procedures vs functions, particularly regarding single composite returns
- Supports polymorphic types but requires actual resolved types for validation
- Integrates with PostgreSQL's type coercion system for maximum compatibility
- Critical for maintaining type safety in SQL function execution
- The function may inject NULL columns to handle dropped attributes in composite types
- Used extensively in both validation and optimization phases of query processing