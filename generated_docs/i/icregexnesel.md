# icregexnesel

## Location
[src/backend/utils/adt/like_support.c:848-856](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L848-L856)

## Overview
A selectivity estimation function for case-insensitive regular expression non-match operations in PostgreSQL's query planner.

## Definition


## Detailed Description
The  function estimates the selectivity (fraction of rows that will match) for case-insensitive regular expression non-match operations (e.g.,  operator). It serves as a wrapper function that calls the generic  function with specific parameters for case-insensitive regex patterns and negation. This function is used by PostgreSQL's query planner to estimate how many rows will NOT match a case-insensitive regular expression pattern, which helps in choosing optimal query execution plans.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that includes:
  - : PlannerInfo pointer containing planner context
  - : OID of the operator being evaluated
  - : List of operator arguments
  - : Variable relation ID for statistics lookup
  - : Collation information

## Dependencies
- Functions called/Symbols referenced:
  -  - Generic pattern selectivity estimation function
  -  - Enum value for case-insensitive regex pattern type
- Called from (representative examples):
  - No direct references found (likely called via function pointer from operator catalog)

## Notes and Other Information
- Returns a float8 value representing the estimated selectivity (0.0 to 1.0)
- The  parameter passed to  indicates this is for a negated match (NOT operation)
- Part of PostgreSQL's statistical estimation system for query optimization
- Located in 