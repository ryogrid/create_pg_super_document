# tsquery_phrase

## Location
[src/backend/utils/adt/tsquery_op.c:150-158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_op.c#L150-L158)

## Overview
Creates a phrase query by joining two tsquery operands with a default distance of 1, ensuring they appear consecutively in the searched text.

## Definition


## Detailed Description
The  function is a PostgreSQL SQL-callable function that creates a phrase query from two tsquery operands. It acts as a wrapper around , hardcoding the distance parameter to 1. This function implements the  operator for text search queries, which requires that the left operand appears exactly one position before the right operand in the document.

The function uses PostgreSQL's function call interface (PG_FUNCTION_ARGS) to accept two tsquery parameters and returns a new tsquery that represents the phrase combination. It delegates all actual work to  with a fixed distance of 1.

## Parameters / Member Variables
- : First tsquery operand (left side of the phrase)
- : Second tsquery operand (right side of the phrase)
- Implicit distance parameter: Fixed at 1 (consecutive positioning)

## Dependencies
- Functions called/Symbols referenced:
  - DirectFunctionCall3
  - [tsquery_phrase_distance](tsquery_phrase_distance.md)
  - PG_RETURN_DATUM
  - PG_GETARG_DATUM
  - [Int32GetDatum](../I/Int32GetDatum.md)

## Notes and Other Information
- This function is a convenience wrapper that provides the most common use case of phrase queries (consecutive terms)
- For custom distances between terms, users should use the  function directly
- The function follows PostgreSQL's fmgr (function manager) calling convention
- Part of PostgreSQL's full-text search functionality
- Maps to the  operator in tsquery syntax