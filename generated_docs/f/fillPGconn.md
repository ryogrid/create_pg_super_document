# fillPGconn

## Location
[src/interfaces/libpq/fe-connect.c:918-955](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L918-L955)

## Overview
Transfers parsed connection option values from a PQconninfoOption array into the appropriate fields of a PGconn structure during connection initialization.

## Definition
```c
static bool fillPGconn(PGconn *conn, PQconninfoOption *connOptions)
```

## Detailed Description
fillPGconn is an internal utility function that performs the critical task of populating a PGconn structure with connection parameters that have been parsed from connection strings or parameter arrays. It iterates through the predefined PQconninfoOptions table, which maps connection parameter keywords to their corresponding offset locations within the PGconn structure. For each valid connection option that has a defined offset and a non-null value, the function dynamically allocates memory and copies the parameter value to the correct field in the connection structure. The function includes error handling for memory allocation failures and maintains proper memory management by freeing any existing values before assigning new ones.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn structure to be populated with connection parameters
- `connOptions`: Array of parsed connection options containing keyword-value pairs

## Dependencies
- Functions called/Symbols referenced:
  - PQconninfoOptions (global options table)
  - internalPQconninfoOption (structure type)
  - [conninfo_getval](../c/conninfo_getval.md)
  - strdup (standard library)
  - free (standard library)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
- Called from (representative examples):
  - [PQconnectStartParams](../P/PQconnectStartParams.md) (fe-connect.c)
  - [connectOptions1](../c/connectOptions1.md) (fe-connect.c)

## Notes and Other Information
- This is a static internal function not exposed in the public libpq API
- Uses structure member offsets to dynamically assign values to PGconn fields
- Performs proper memory management by freeing existing values before assignment
- Returns false and sets error messages if memory allocation fails during value copying
- The comment warns against putting "intelligence" in this function - complex logic belongs in pqConnectOptions2
- Essential step in the connection initialization process for transferring parsed parameters to the working connection structure
- Handles all standard PostgreSQL connection parameters including host, port, database, user, etc.
- Memory allocation failures are properly propagated up the call stack through the return value

## Simplified Source

```c
static bool fillPGconn(PGconn *conn, PQconninfoOption *connOptions)
{
    // Iterate through all possible connection options
    for (const internalPQconninfoOption *option = PQconninfoOptions;
         option->keyword; option++) {

        // Skip options that don't map to PGconn structure fields
        if (option->connofs >= 0) {
            const char *tmp = conninfo_getval(connOptions, option->keyword);

            if (tmp) {
                // Calculate pointer to the PGconn structure field
                char **connmember = (char **) ((char *) conn + option->connofs);

                // Free existing value and assign new one
                free(*connmember);
                *connmember = strdup(tmp);

                // Handle memory allocation failure
                if (*connmember == NULL) {
                    libpq_append_conn_error(conn, "out of memory");
                    return false;
                }
            }
        }
    }

    return true;
}
```