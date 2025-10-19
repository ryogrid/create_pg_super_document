# libpq_fetch_file

## Location
[src/bin/pg_rewind/libpq_source.c:635-674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/libpq_source.c#L635-L674)

## Overview
Fetches a single file from a remote PostgreSQL server and returns it as a malloc'd buffer using the pg_read_binary_file function.

## Definition
```c
static char *libpq_fetch_file(rewind_source *source, const char *path, size_t *filesize)
```

## Detailed Description
This function retrieves a file from a remote PostgreSQL cluster by executing the pg_read_binary_file SQL function via libpq. It takes a rewind_source (which contains a database connection), a file path, and an optional pointer to store the file size. The function uses parameterized queries for safety and returns the file contents as a null-terminated string in a malloc'd buffer.

The function performs error checking to ensure the SQL query succeeds and returns exactly one tuple with non-null data. It handles memory allocation for the result buffer and adds a null terminator for string safety, even though the file might be binary data.

## Parameters / Member Variables
- `source`: Pointer to rewind_source structure containing the database connection
- `path`: File path on the remote server to fetch
- `filesize`: Optional output parameter to receive the size of the fetched file (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [PQexecParams](../P/PQexecParams.md) (executes parameterized SQL query)
  - [PQresultStatus](../P/PQresultStatus.md) (checks query result status)
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md) (gets error message from failed query)
  - [PQntuples](../P/PQntuples.md) (gets number of result tuples)
  - [PQgetisnull](../P/PQgetisnull.md) (checks if result value is NULL)
  - [PQgetlength](../P/PQgetlength.md) (gets length of result value)
  - [PQgetvalue](../P/PQgetvalue.md) (gets result value data)
  - [PQclear](../P/PQclear.md) (cleans up result)
  - [pg_malloc](../p/pg_malloc.md) (PostgreSQL memory allocation)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL fatal error reporting)
  - pg_log_debug (PostgreSQL debug logging)
- Called from (representative examples):
  - [init_libpq_source](../i/init_libpq_source.md) (src/bin/pg_rewind/libpq_source.c:91)

## Notes and Other Information
- This is a static function used internally within the pg_rewind utility
- The function uses binary format (format code 1) for the SQL result to handle binary files correctly
- Memory allocated by this function must be freed by the caller using pg_free or free
- The function adds a null terminator even for binary files, which is safe but may not be necessary for all use cases
- Error handling uses pg_fatal which terminates the program on failure
- The function logs successful file fetches at debug level for troubleshooting purposes

## Simplified Source

```c
static char *
libpq_fetch_file(rewind_source *source, const char *path, size_t *filesize)
{
    PGconn *conn = ((libpq_source *) source)->conn;
    PGresult *res;
    char *result;
    int len;
    const char *paramValues[1];

    // Execute pg_read_binary_file with file path parameter
    paramValues[0] = path;
    res = PQexecParams(conn, "SELECT pg_read_binary_file($1)",
                       1, NULL, paramValues, NULL, NULL, 1);

    // Check for query errors
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        pg_fatal("could not fetch remote file \"%s\": %s",
                 path, PQresultErrorMessage(res));

    // Validate result - should have exactly one non-null tuple
    if (PQntuples(res) != 1 || PQgetisnull(res, 0, 0))
        pg_fatal("unexpected result set while fetching remote file \"%s\"",
                 path);

    // Copy file data to malloc'd buffer
    len = PQgetlength(res, 0, 0);
    result = pg_malloc(len + 1);
    memcpy(result, PQgetvalue(res, 0, 0), len);
    result[len] = '\0';  // Null terminate for safety

    PQclear(res);

    pg_log_debug("fetched file \"%s\", length %d", path, len);

    // Return file size if requested
    if (filesize)
        *filesize = len;

    return result;
}
```