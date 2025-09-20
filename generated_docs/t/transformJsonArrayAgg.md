# transformJsonArrayAgg

## Location
[src/backend/parser/parse_expr.c:3972-4009](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L3972-L4009)

## Overview
Transforms JSON_ARRAYAGG() aggregate function expressions into JsonConstructorExpr nodes that aggregate values into JSON arrays.

## Definition

```c
structorOutput(pstate, agg->constructor->output,
											   list_make1(arg));
```
## Detailed Description
This function transforms JSON_ARRAYAGG() expressions into PostgreSQL's internal representation for JSON array aggregation. The transformation is simpler than JSON_OBJECTAGG since it only deals with single values rather than key-value pairs, and doesn't support unique constraints (arrays can contain duplicate values).

The function maps to runtime functions:
- For JSON format: json_agg[_strict]
- For JSONB format: jsonb_agg[_strict]

The transformation process includes:
1. Transforming the value argument using specialized JSON value transformation with default formatting
2. Processing output formatting and return type specifications
3. Selecting the correct aggregate function based on format type and null handling:
   - Standard variants include null values in the array
   -  variants implement absent-on-null behavior, excluding null values
4. Delegating to the common JSON aggregate constructor with a single-element argument list

Unlike JSON_OBJECTAGG, this function doesn't support unique constraints since array elements can legitimately be duplicated.

## Parameters / Member Variables
- : ParseState pointer containing current parsing context and state information for the transformation
- : JsonArrayAgg pointer containing the source JSON_ARRAYAGG() expression with value argument and behavioral options

## Dependencies
- Functions called/Symbols referenced:
  - [transformJsonValueExpr](transformJsonValueExpr.md) (for value expression transformation with JSON_ARRAYAGG context)
  - [transformJsonConstructorOutput](transformJsonConstructorOutput.md) (for output formatting)
  - [transformJsonAggConstructor](transformJsonAggConstructor.md) (for common aggregate constructor logic)
  - list_make1 (for creating single-element argument list)
  - JS_FORMAT_DEFAULT, JS_FORMAT_JSONB (format type constants)
  - F_JSON_AGG*, F_JSONB_AGG* (aggregate function OID constants)
  - JSCTOR_JSON_ARRAYAGG (constructor type constant)
  - JSONOID, JSONBOID (type OID constants)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md) (main expression transformation dispatcher)

## Notes and Other Information
- The function supports only two variants for each format based on null handling:
  - Standard: includes null values in the resulting array
  - Strict: excludes null values from the array (absent-on-null behavior)
- Unlike JSON_OBJECTAGG, no unique constraint enforcement is needed since arrays can contain duplicates
- The unique parameter is always passed as false to the common aggregate constructor
- The choice between JSON and JSONB output formats affects both the aggregate function and the result type
- Value expressions receive specialized JSON formatting treatment appropriate for array elements
- The function handles both regular aggregates and window function contexts through the common aggregate constructor
- All location and error context information is preserved through the transformation chain