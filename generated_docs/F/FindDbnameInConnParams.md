# FindDbnameInConnParams

## Location
[src/bin/pg_basebackup/streamutil.c:282-307](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/streamutil.c#L282-L307)

## Overview
A helper function that extracts the database name value from a PQconninfoOption parameter array.

## Definition

```c
static char *
FindDbnameInConnParams(PQconninfoOption *conn_opts)
```
## Detailed Description
FindDbnameInConnParams is a static helper function that searches through an array of PostgreSQL connection parameters to locate and extract the "dbname" parameter value. It iterates through the PQconninfoOption array, comparing each keyword with "dbname" and returns a duplicated copy of the value if found. The function ensures the value is not NULL or empty before returning it.

## Parameters / Member Variables
- `*conn_opts`: Pointer to an array of PQconninfoOption structures containing connection parameters
## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md)
  - strcmp
  - [PQconninfoOption](../P/PQconninfoOption.md) (type)
- Called from (representative examples):
  - [GetDbnameFromConnectionOptions](../G/GetDbnameFromConnectionOptions.md)

## Notes and Other Information
- Returns a strdup'd copy of the dbname value, requiring the caller to free the memory
- Returns NULL if no dbname parameter is found or if the value is empty
- This is a static function, only accessible within streamutil.c
- Designed specifically as a helper for GetDbnameFromConnectionOptions function

## Simplified Source

```c
/*
 * FindDbnameInConnParams
 *
 * Extract the value of dbname from PQconninfoOption parameters.
 * Returns a strdup'd result or NULL.
 */
static char *
FindDbnameInConnParams(PQconninfoOption *conn_opts)
{
    PQconninfoOption *conn_opt;

    // Search through connection options for dbname parameter
    for (conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++) {
        if (strcmp(conn_opt->keyword, "dbname") == 0 &&
            conn_opt->val != NULL && conn_opt->val[0] != '\0') {
            return pg_strdup(conn_opt->val);
        }
    }

    return NULL;  // dbname not found or empty
}
```