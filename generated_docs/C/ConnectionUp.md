# ConnectionUp

## Location
[src/bin/psql/common.c:324-341](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L324-L341)

## Overview
ConnectionUp is a static utility function that checks whether the backend PostgreSQL connection is still active and available.

## Definition

```c
static bool
ConnectionUp(void)
```
## Detailed Description
ConnectionUp provides a simple way to verify the status of the current PostgreSQL database connection in psql. It acts as a wrapper around the libpq PQstatus() function, checking if the connection stored in the global pset.db variable is still valid. The function returns true if the connection is in any state other than CONNECTION_BAD, indicating that the connection is up and potentially usable for database operations. This function is commonly used throughout psql to validate connection state before attempting database operations or to provide appropriate error handling when the connection has been lost.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PQstatus (libpq function to get connection status)
  - CONNECTION_BAD (libpq constant representing a bad connection state)
  - pset.db (global variable containing the database connection)
- Called from (representative examples):
  - [CheckConnection](CheckConnection.md) (uses this to verify connection status)
  - [SendQuery](../S/SendQuery.md) (checks connection before sending queries)

## Notes and Other Information
- This is a static function, only accessible within the common.c compilation unit
- Provides a boolean abstraction over the more complex libpq connection status values
- Part of psql's connection management and error handling infrastructure
- The function relies on the global pset structure which contains psql's runtime state
- Simple but critical for maintaining robust database connectivity in interactive sessions