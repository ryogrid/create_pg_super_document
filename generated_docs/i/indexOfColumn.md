# indexOfColumn

## Location
[src/bin/psql/crosstabview.c:636-694](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/crosstabview.c#L636-L694)

## Overview
Resolves a column reference (either a numeric index or column name) to a zero-based column index in a PostgreSQL result set.

## Definition

```c
static int
indexOfColumn(char *arg, const PGresult *res)
```
## Detailed Description
This function provides flexible column reference resolution for PostgreSQL's psql \crosstabview feature. It accepts either numeric column references (1-based) or column names and converts them to zero-based indices. For numeric inputs, it validates the range against the result set's field count. For name-based inputs, it performs case-insensitive matching after dequoting and downcasing the identifier, ensuring compatibility with PostgreSQL's identifier handling rules.

The function includes comprehensive error handling for various failure scenarios: out-of-range column numbers, ambiguous column names (when multiple columns have the same name), and non-existent column names. It may modify the input string during processing when handling quoted identifiers.

## Parameters / Member Variables
- `arg`: String containing either a column number (1-based) or column name to resolve
- `res`: PostgreSQL result set containing the column information to search

## Dependencies
- Functions called/Symbols referenced:
  - [PQnfields](../P/PQnfields.md) (gets the number of fields/columns in the result set)
  - [PQfname](../P/PQfname.md) (gets the column name for a given field index)
  - dequote_downcase_identifier (processes quoted identifiers following PostgreSQL rules)
  - pg_log_error (reports error messages)
- Called from (representative examples):
  - [PrintResultInCrosstab](../P/PrintResultInCrosstab.md) (main crosstab processing function for resolving column references)

## Notes and Other Information
- Returns -1 on any error condition (out of range, ambiguous, or not found)
- Supports both 1-based numeric column references and case-insensitive name matching
- May modify the contents of the arg string during identifier processing
- Handles quoted column names by dequoting them before comparison
- Validates that numeric column references fall within the valid range [1, PQnfields(res)]
- Detects and reports ambiguous column names when multiple columns share the same name
- Uses PostgreSQL's standard identifier case-folding rules for name matching