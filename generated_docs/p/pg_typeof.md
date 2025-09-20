# pg_typeof

## Location
[src/backend/utils/adt/misc.c:564-582](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L564-L582)

## Overview
Returns the OID of the data type of its argument, providing runtime type information for any PostgreSQL expression.

## Definition
```c
Datum pg_typeof(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides runtime type introspection capabilities by returning the type OID of its argument. It's a simple wrapper around get_fn_expr_argtype() that extracts type information from the function call context.

The function is polymorphic - it can accept any data type as its argument and will return the corresponding type OID. This makes it useful for:

1. **Type debugging**: Determining the actual type of complex expressions
2. **Dynamic type checking**: Runtime verification of expected types
3. **Metadata queries**: Building dynamic queries that adapt based on column types
4. **Type introspection**: Understanding the types produced by functions or expressions

The returned OID can be used with other system functions or joined with pg_type to get human-readable type names and additional type metadata.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md)
  - PG_RETURN_OID
- Accessed through:
  - fcinfo->flinfo (function call information structure)
- Called from:
  - SQL function calls (no direct C references found)

## Notes and Other Information
- This is a polymorphic function that can accept arguments of any PostgreSQL data type
- The function examines type information from the expression tree, not the runtime value
- Commonly used in SQL queries for type debugging: SELECT pg_typeof(column_name) FROM table_name
- The returned OID can be joined with pg_type.oid to get readable type names
- Particularly useful in PL/pgSQL and other procedural languages for dynamic type handling
- The function is very lightweight as it only accesses metadata, not actual data values
- Type resolution happens at query planning time, so the result is determined before execution
- Can be used with complex expressions: SELECT pg_typeof(column1 + column2) to determine result types