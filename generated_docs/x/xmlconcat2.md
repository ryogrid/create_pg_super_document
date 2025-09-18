# xmlconcat2

## Location
src/backend/utils/adt/xml.c: 619 - 636

## Overview
A two-argument wrapper function for xmlconcat that handles NULL values and supports the XMLAGG aggregate function.

## Definition
```c
Datum xmlconcat2(PG_FUNCTION_ARGS)
```

## Detailed Description
The xmlconcat2 function serves as a two-parameter interface to the more general xmlconcat function, specifically designed to support PostgreSQL's XMLAGG aggregate function. It implements the following NULL-handling logic:

1. If both arguments are NULL, returns NULL
2. If the first argument is NULL, returns the second argument
3. If the second argument is NULL, returns the first argument  
4. If both arguments are non-NULL, calls xmlconcat with both values

This function is essential for aggregate operations where partial results need to accumulate incrementally, with proper handling of NULL states during the aggregation process.

## Parameters / Member Variables
- First XML parameter (accessed via `PG_GETARG_XML_P(0)`): First XML value to concatenate
- Second XML parameter (accessed via `PG_GETARG_XML_P(1)`): Second XML value to concatenate

## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_XML_P
  - PG_GETARG_XML_P
  - [xmlconcat](xmlconcat.md)
  - list_make2
- Called from (representative examples):
  - No direct references found in codebase (likely used by aggregate function system)

## Notes and Other Information
- Primary purpose is to support XMLAGG aggregate function implementation
- Provides NULL-safe concatenation of exactly two XML values
- Uses the more general xmlconcat function for the actual concatenation work
- Part of PostgreSQL's SQL standard XML aggregate function support
- Follows PostgreSQL's function calling convention with PG_FUNCTION_ARGS