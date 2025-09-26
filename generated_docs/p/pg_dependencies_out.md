# pg_dependencies_out

## Location
[src/backend/statistics/dependencies.c:670-709](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L670-L709)

## Overview
This function serves as the output routine for the pg_dependencies data type, converting binary dependency statistics into a human-readable string representation for display purposes.

## Definition

```c
Datum
pg_dependencies_out(PG_FUNCTION_ARGS)
```
## Detailed Description
The function takes binary pg_dependencies data and produces a formatted string representation showing functional dependencies between columns. It deserializes the binary data into an MVDependencies structure, then iterates through each dependency to build a JSON-like string format.

Each dependency is formatted as a quoted string showing the attributes involved in the dependency relationship, with the dependent attributes listed first, followed by " => " and the determining attribute, concluded with the dependency degree (strength) as a floating-point value. Multiple dependencies are comma-separated within curly braces.

The output format follows the pattern: {"attr1, attr2 => attr3": degree, "attr4 => attr5": degree, ...}

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP (extract bytea argument)
  - [statext_dependencies_deserialize](../s/statext_dependencies_deserialize.md) (convert binary to MVDependencies)
  - [initStringInfo](../i/initStringInfo.md) (initialize string buffer)
  - [appendStringInfoChar](../a/appendStringInfoChar.md) (append single character)
  - [appendStringInfoString](../a/appendStringInfoString.md) (append string literal)
  - [appendStringInfo](../a/appendStringInfo.md) (append formatted string)
  - PG_RETURN_CSTRING (return C string result)

- Called from (representative examples):
  - Not directly called (registered as type output function in system catalogs)

## Notes and Other Information
- Part of PostgreSQL's type system infrastructure for pg_dependencies
- Provides human-readable representation for debugging and inspection
- Uses StringInfo for efficient string building
- Dependencies are displayed in attribute number format (not column names)
- The degree value represents the strength of the functional dependency (0.0 to 1.0)
- Registered automatically when the pg_dependencies type is defined in the system