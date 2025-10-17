# concat_conninfo_dbname

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:409-432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L409-L432)

## Overview
Appends a database name to a base connection string to build a complete PostgreSQL connection string suitable for establishing database connections.

## Definition

```c
static char *
concat_conninfo_dbname(const char *conninfo, const char *dbname)
```
## Detailed Description
The  function is a utility for constructing complete PostgreSQL connection strings by adding a database name to a base connection string. This design pattern is used in pg_createsubscriber because the database name is the only parameter that typically changes between different connection attempts, while other connection parameters (host, port, user credentials, etc.) remain constant.

The function uses PostgreSQL's PQExpBuffer for safe string manipulation and the  function to properly format and escape the database name parameter according to PostgreSQL connection string standards. This ensures that special characters in database names are handled correctly and the resulting connection string is valid.

## Parameters
- : The base connection string containing all connection parameters except the database name
- : The database name to append to the connection string (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  -  - Creates a new expandable string buffer
  -  - Appends the base connection string to the buffer
  -  - Safely appends the database name as a connection parameter
  -  - Creates a duplicate of the resulting connection string
  -  - Frees the temporary buffer memory
- Called from:
  -  structure initialization
  -  function (twice for different database connections)

## Notes and Other Information
- The function is marked as , indicating it's only used within the pg_createsubscriber.c file
- Uses assertion to ensure the base connection string is not NULL
- Handles NULL database names gracefully through 
- Returns a newly allocated string that must be freed by the caller
- The design allows for efficient reuse of base connection parameters while varying only the target database
- Proper escaping and formatting of connection string parameters is handled automatically by 
- Memory management is handled safely with proper buffer cleanup

## Simplified Source

```c
static char *
concat_conninfo_dbname(const char *conninfo, const char *dbname)
{
    // Create buffer for building connection string
    PQExpBuffer buf = createPQExpBuffer();

    // Add base connection parameters
    appendPQExpBufferStr(buf, conninfo);

    // Append database name parameter
    appendConnStrItem(buf, "dbname", dbname);

    // Return completed connection string
    char *result = pg_strdup(buf->data);
    destroyPQExpBuffer(buf);

    return result;
}
```