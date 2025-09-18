# time_support

## Location
[src/backend/utils/adt/date.c:1605-1624](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L1605-L1624)

## Overview
A planner support function that provides optimization support for the time_scale() and timetz_scale() length coercion functions in PostgreSQL's query planning phase.

## Definition


## Detailed Description
The time_support function serves as a planner support function specifically designed to assist the PostgreSQL query planner in optimizing calls to time_scale() and timetz_scale() functions. These are length coercion functions used for TIME and TIMETZ data types. The function implements simplification logic for temporal operations by delegating to the TemporalSimplify function when it receives a SupportRequestSimplify request. This allows the planner to potentially simplify or optimize expressions involving time scaling operations during query planning.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function call information
  - : A Node pointer representing the support request from the planner

## Dependencies
- Functions called/Symbols referenced:
  - SupportRequestSimplify (struct type for simplification requests)
  - [TemporalSimplify](../T/TemporalSimplify.md) (function that performs the actual temporal simplification)
  - MAX_TIME_PRECISION (constant defining maximum time precision)
- Called from (representative examples):
  - No direct references found in the codebase (likely registered as a support function)

## Notes and Other Information
- This function is specifically mentioned to handle both time_scale() and timetz_scale() without needing to distinguish between them
- The function only handles SupportRequestSimplify request types, returning NULL for other request types
- Located in src/backend/utils/adt/date.c:1605-1624
- Part of PostgreSQL's support function infrastructure for query optimization