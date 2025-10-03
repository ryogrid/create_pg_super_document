# printResultSet

## Location
[src/test/isolation/isolationtester.c:1113-1125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/isolation/isolationtester.c#L1113-L1125)

## Overview
Formats and prints a PostgreSQL query result set to stdout with headers and aligned columns for isolation test output.

## Definition

```c
static void
printResultSet(PGresult *res)
```
## Detailed Description
This utility function provides standardized formatting for displaying PostgreSQL query results in isolation tests. It configures the libpq PQprint function to display results in a table format with headers, aligned columns, and pipe (|) separators between fields. This creates consistent, readable output for test results that can be easily compared across test runs.

The function uses the PQprintOpt structure to control formatting options, ensuring that all result sets are displayed with the same visual structure regardless of their content or source query.

## Parameters / Member Variables
- `*res`: Pointer to a PGresult structure containing the query results to be displayed
## Dependencies
- Functions called/Symbols referenced:
  - PQprintOpt (structure for print options)
  - PQprint (libpq function for formatted result output)
  - memset (standard library function)
- Called from (representative examples):
  - [try_complete_step](../t/try_complete_step.md)
  - [run_permutation](../r/run_permutation.md)

## Notes and Other Information
- Outputs results to stdout for inclusion in test output files
- Uses pipe (|) as field separator for clear column delineation
- Enables both headers and column alignment for maximum readability
- Essential for producing consistent, parseable test output across different isolation test scenarios
- Part of the standardized output formatting infrastructure for PostgreSQL isolation testing