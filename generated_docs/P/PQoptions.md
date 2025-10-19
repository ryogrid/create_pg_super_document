# PQoptions

## Location
[src/interfaces/libpq/fe-connect.c:7098-7105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7098-L7105)

## Overview
PQoptions returns the command-line options that were passed when establishing a PostgreSQL database connection.

## Definition
```c
char *PQoptions(const PGconn *conn)
```

## Detailed Description
This function retrieves the command-line options string that was used when the PostgreSQL connection was established. The options are stored in the connection object and represent various parameters that control the behavior of the connection. These options are typically passed through connection strings or environment variables when the connection is created.

The function provides access to the pgoptions field of the PGconn structure, which contains the options string exactly as it was specified during connection establishment.

## Parameters / Member Variables
- `conn`: A pointer to a PGconn structure representing the database connection. If NULL, the function returns NULL.

## Dependencies
- Functions called/Symbols referenced:
  - ConnStatusType (referenced in nearby code)
- Called from (representative examples):
  - PQsetdb (in libpq-fe.h header as part of the API)

## Notes and Other Information
- Returns NULL if the connection pointer is NULL
- Returns the pgoptions field directly from the connection structure
- The returned string should not be modified by the caller
- The options string reflects the state at connection time and may include various PostgreSQL server options
- This is part of the standard libpq connection information API
- The returned pointer remains valid as long as the PGconn object exists

## Simplified Source

```c
char *PQoptions(const PGconn *conn) {
    // Safety check for null connection
    if (!conn)
        return NULL;

    // Return the PostgreSQL options string
    return conn->pgoptions;
}
```