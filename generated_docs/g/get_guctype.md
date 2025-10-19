# get_guctype

## Location
[src/bin/psql/tab-complete.c:6380-6415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L6380-L6415)

## Overview
Retrieves the data type of a PostgreSQL GUC (Grand Unified Configuration) variable by querying the pg_settings system catalog.

## Definition
```c
static char *
get_guctype(const char *varname)
```

## Detailed Description
The `get_guctype` function performs a database query to determine the data type of a specified PostgreSQL configuration parameter (GUC variable). It queries the `pg_catalog.pg_settings` system view to retrieve the `vartype` column for the given parameter name. The function:

1. **Input Sanitization**: Uses `escape_string` to safely escape the variable name for SQL injection protection
2. **Query Construction**: Builds a SQL query using PQExpBuffer to query pg_settings with case-insensitive name matching
3. **Query Execution**: Uses `exec_query` to execute the constructed query safely
4. **Result Processing**: Extracts the variable type if the query succeeds and returns results
5. **Memory Management**: Properly cleans up all allocated resources including buffers and result sets

The query uses case-insensitive comparison to match GUC variable names, making it robust against case variations in user input.

## Parameters / Member Variables
- `varname`: The name of the GUC variable to look up

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (structure for building SQL queries)
  - [escape_string](../e/escape_string.md) (for SQL injection protection)
  - [initPQExpBuffer](../i/initPQExpBuffer.md) (initialize query buffer)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) (build SQL query)
  - [exec_query](../e/exec_query.md) (execute the query safely)
  - [termPQExpBuffer](../t/termPQExpBuffer.md) (cleanup query buffer)
  - PGRES_TUPLES_OK (constant for successful query result)
  - [PQntuples](../P/PQntuples.md) (check number of result rows)
  - [pg_strdup](../p/pg_strdup.md) (duplicate result string)
  - [PQgetvalue](../P/PQgetvalue.md) (extract result value)
  - [PQclear](../P/PQclear.md) (cleanup result set)
- Called from (representative examples):
  - THING_NO_SHOW (completion handling)
  - HeadMatchesCS (case-sensitive header matching for GUC completion)

## Notes and Other Information
- Returns malloc'd string containing the GUC type, or NULL if variable is unknown
- Caller is responsible for freeing the returned string
- Part of psql's tab completion system in PostgreSQL
- Located in src/bin/psql/tab-complete.c at lines 6380-6415
- The function is static, meaning it's only accessible within the tab-complete.c file
- Uses case-insensitive matching for robustness with user input
- Queries the pg_catalog.pg_settings system view for configuration metadata
- Properly handles SQL injection protection through parameter escaping
- Returns NULL for unknown GUC variables, allowing caller to handle gracefully

## Simplified Source

```c
static char *
get_guctype(const char *varname)
{
    PQExpBufferData query_buffer;
    char *e_varname;
    PGresult *result;
    char *guctype = NULL;

    // Escape variable name for safe SQL construction
    e_varname = escape_string(varname);

    // Build query to get GUC variable type from pg_settings
    initPQExpBuffer(&query_buffer);
    appendPQExpBuffer(&query_buffer,
                      "SELECT vartype FROM pg_catalog.pg_settings "
                      "WHERE pg_catalog.lower(name) = pg_catalog.lower('%s')",
                      e_varname);

    // Execute query and clean up query buffer
    result = exec_query(query_buffer.data);
    termPQExpBuffer(&query_buffer);
    free(e_varname);

    // Extract type if query succeeded and returned results
    if (PQresultStatus(result) == PGRES_TUPLES_OK && PQntuples(result) > 0)
        guctype = pg_strdup(PQgetvalue(result, 0, 0));

    PQclear(result);
    return guctype;
}
```