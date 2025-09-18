# transformJsonObjectAgg

## Location
src/backend/parser/parse_expr.c: 3908 - 3971

## Overview
Transforms JSON_OBJECTAGG() aggregate function expressions into JsonConstructorExpr nodes that aggregate key-value pairs into JSON objects.

## Definition


## Detailed Description
This function transforms JSON_OBJECTAGG() expressions into PostgreSQL's internal representation for JSON object aggregation. The transformation process selects the appropriate underlying aggregate function based on the output format (JSON vs JSONB) and behavioral flags (unique key enforcement and null handling).

The function maps to runtime functions:
- For JSON format: json_object_agg[_unique][_strict]
- For JSONB format: jsonb_object_agg[_unique][_strict]

The transformation process includes:
1. Transforming the key expression using standard expression transformation
2. Transforming the value expression using specialized JSON value transformation with default formatting
3. Creating a two-element argument list (key, value)
4. Processing output formatting and return type specifications
5. Selecting the correct aggregate function based on format type and behavioral flags:
   -  variants enforce unique keys within the aggregate
   -  variants implement absent-on-null behavior
6. Delegating to the common JSON aggregate constructor with appropriate parameters

## Parameters / Member Variables
- : ParseState pointer containing current parsing context and state information for the transformation
- : JsonObjectAgg pointer containing the source JSON_OBJECTAGG() expression with key-value arguments and behavioral options

## Dependencies
- Functions called/Symbols referenced:
  - transformExprRecurse (for key expression transformation)
  - transformJsonValueExpr (for value expression transformation with JSON_OBJECTAGG context)
  - transformJsonConstructorOutput (for output formatting)
  - transformJsonAggConstructor (for common aggregate constructor logic)
  - list_make2 (for creating key-value argument pairs)
  - JS_FORMAT_DEFAULT, JS_FORMAT_JSONB (format type constants)
  - F_JSON_OBJECT_AGG*, F_JSONB_OBJECT_AGG* (aggregate function OID constants)
  - JSCTOR_JSON_OBJECTAGG (constructor type constant)
  - JSONOID, JSONBOID (type OID constants)
- Called from (representative examples):
  - transformExprRecurse (main expression transformation dispatcher)

## Notes and Other Information
- The function supports four variants for each format based on behavioral flags:
  - Standard: basic object aggregation allowing duplicate keys and including null values
  - Unique: enforces unique keys, raises error on duplicates
  - Strict: excludes null values from the result (absent-on-null behavior)
  - Unique + Strict: combines both unique key enforcement and null exclusion
- The choice between JSON and JSONB output formats affects both the aggregate function and the result type
- Key expressions are transformed normally while value expressions receive special JSON formatting treatment
- All location and error context information is preserved through the transformation chain
- The function handles both regular aggregates and window function contexts through the common aggregate constructor