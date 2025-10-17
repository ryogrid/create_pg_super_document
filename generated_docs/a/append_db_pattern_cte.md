# append_db_pattern_cte

## Location
[src/bin/pg_amcheck/pg_amcheck.c:1537-1582](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L1537-L1582)

## Overview
Constructs a Common Table Expression (CTE) containing database name patterns extracted from a pattern array for SQL query generation in pg_amcheck.

## Definition

```c
static bool
append_db_pattern_cte(PQExpBuffer buf, const PatternInfoArray *pia,
					  PGconn *conn, bool inclusive)
```
## Detailed Description
This function generates the body of a SQL CTE (Common Table Expression) that contains database patterns filtered from the input pattern array. The CTE produces two columns: `pattern_id` (index in the pattern array) and `rgx` (the database regular expression). The function provides flexibility in pattern inclusion based on the `inclusive` parameter - when false, it only includes patterns that specify only a database name, when true, it includes patterns that may also have schema and/or relation components.

## Parameters / Member Variables
- `buf`: PQExpBuffer to append the CTE SQL content to
- `pia`: Pointer to PatternInfoArray containing the patterns to process
- `conn`: PostgreSQL connection handle used for proper string literal escaping
- `inclusive`: Boolean flag controlling whether to include patterns with schema/relation parts

## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBufferStr](appendPQExpBufferStr.md)
  - [appendPQExpBuffer](appendPQExpBuffer.md)
  - [appendStringLiteralConn](appendStringLiteralConn.md)
  - [appendPQExpBufferChar](appendPQExpBufferChar.md)
  - [PatternInfoArray](../P/PatternInfoArray.md)
  - [PatternInfo](../P/PatternInfo.md)
- Called from (representative examples):
  - [compile_database_list](../c/compile_database_list.md) (at src/bin/pg_amcheck/pg_amcheck.c:1608)
  - [compile_database_list](../c/compile_database_list.md) (at src/bin/pg_amcheck/pg_amcheck.c:1625)

## Notes and Other Information
- Returns true if any database patterns were found and appended, false otherwise
- When no patterns are found, appends a dummy SELECT that returns no rows to maintain valid SQL syntax
- Uses proper SQL string literal escaping via appendStringLiteralConn to prevent injection issues
- Part of pg_amcheck's database discovery mechanism for pattern-based object selection
- The generated CTE is typically used in larger SQL queries to match database names against user-specified patterns

## Simplified Source

```c
static bool append_db_pattern_cte(PQExpBuffer buf, const PatternInfoArray *pia,
                                  PGconn *conn, bool inclusive) {
    const char *comma = "";
    bool have_values = false;

    // Process each pattern in the array
    for (int pattern_id = 0; pattern_id < pia->len; pattern_id++) {
        PatternInfo *info = &pia->data[pattern_id];

        // Include pattern if it has database regex and meets inclusion criteria
        if (info->db_regex != NULL &&
            (inclusive || (info->nsp_regex == NULL && info->rel_regex == NULL))) {

            // Add VALUES clause on first match
            if (!have_values)
                appendPQExpBufferStr(buf, "\nVALUES");

            have_values = true;

            // Add pattern_id and escaped database regex
            appendPQExpBuffer(buf, "%s\n(%d, ", comma, pattern_id);
            appendStringLiteralConn(buf, info->db_regex, conn);
            appendPQExpBufferChar(buf, ')');
            comma = ",";
        }
    }

    // If no patterns found, add dummy SELECT to maintain valid SQL
    if (!have_values)
        appendPQExpBufferStr(buf, "\nSELECT NULL, NULL, NULL WHERE false");

    return have_values;
}
```