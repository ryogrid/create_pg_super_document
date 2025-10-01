# pqSaveParameterStatus

## Location
[src/interfaces/libpq/fe-exec.c:1081-1205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L1081-L1205)

## Overview
pqSaveParameterStatus stores and manages server parameter status information received from the PostgreSQL backend, maintaining a linked list of parameter name-value pairs and updating connection-specific cached values for critical parameters.

## Definition

```c
void
pqSaveParameterStatus(PGconn *conn, const char *name, const char *value)
```
## Detailed Description
This function manages the storage of server parameter status information in a PGconn structure. It first removes any existing entry for the same parameter name, then creates a new entry with the updated value. The function uses a space-efficient single malloc allocation that stores the pgParameterStatus structure, parameter name, and value in one contiguous block.

Additionally, the function maintains cached copies of several critical parameters directly in the PGconn structure for performance reasons. These include client_encoding, standard_conforming_strings, server_version (converted to numeric form), default_transaction_read_only, in_hot_standby, and scram_iterations. Some parameters like client_encoding and standard_conforming_strings are also stored in static variables to support legacy functions like PQescapeString and PQescapeBytea in single-connection programs.

## Parameters / Member Variables
- : Pointer to the PGconn structure that will store the parameter status
- : String containing the parameter name (e.g., "client_encoding", "server_version")
- : String containing the parameter value as sent by the server

## Dependencies
- Functions called/Symbols referenced:
  - malloc (for allocating parameter status structures)
  - free (for removing old parameter entries)
  - strcmp (for parameter name comparisons)
  - strcpy (for copying parameter strings)
  - strlen (for calculating string lengths)
  - [pg_char_to_encoding](pg_char_to_encoding.md) (for converting encoding names)
  - sscanf (for parsing server version)
  - atoi (for parsing numeric values)
- Types used:
  - [pgParameterStatus](pgParameterStatus.md) (linked list node structure)
- Constants used:
  - PG_SQL_ASCII (fallback encoding)
  - PG_BOOL_YES, PG_BOOL_NO (boolean value constants)
- Called from:
  - [getParameterStatus](../g/getParameterStatus.md) (in fe-protocol3.c for processing server messages)

## Notes and Other Information
- Uses a linked list to store parameter status information, with new entries prepended to the list
- Implements space-efficient allocation: stores structure, name, and value in a single malloc block
- Automatically removes old entries for the same parameter before adding new ones
- Maintains cached copies of critical parameters in PGconn fields for quick access
- Handles server version parsing for both old format (9.6.1) and new format (10.1) version numbers
- Updates static variables for client_encoding and standard_conforming_strings to support legacy escape functions
- Server version is converted to numeric format: major*10000 + minor*100 + revision for old style, or major*10000 + minor for new style
- [Boolean](../B/Boolean.md) parameters are converted from string "on"/"off" to PG_BOOL_YES/PG_BOOL_NO constants
- Memory allocation failure is handled gracefully - the function continues even if malloc fails
- Parameter names and values are copied, not referenced, ensuring the connection owns the data
- The function supports various PostgreSQL configuration parameters including encoding settings, version info, transaction settings, and authentication parameters

## Simplified Source

```c
void
pqSaveParameterStatus(PGconn *conn, const char *name, const char *value)
{
    pgParameterStatus *pstatus;
    pgParameterStatus *prev;

    // Remove any existing entry for this parameter
    for (pstatus = conn->pstatus, prev = NULL; pstatus != NULL; prev = pstatus, pstatus = pstatus->next) {
        if (strcmp(pstatus->name, name) == 0) {
            if (prev)
                prev->next = pstatus->next;
            else
                conn->pstatus = pstatus->next;
            free(pstatus);
            break;
        }
    }

    // Allocate new entry (structure + name + value in single block)
    pstatus = (pgParameterStatus *) malloc(sizeof(pgParameterStatus) + strlen(name) + strlen(value) + 2);
    if (pstatus) {
        char *ptr = ((char *) pstatus) + sizeof(pgParameterStatus);
        pstatus->name = ptr;
        strcpy(ptr, name);
        ptr += strlen(name) + 1;
        pstatus->value = ptr;
        strcpy(ptr, value);
        pstatus->next = conn->pstatus;
        conn->pstatus = pstatus;
    }

    // Update cached copies of critical parameters
    if (strcmp(name, "client_encoding") == 0) {
        conn->client_encoding = pg_char_to_encoding(value);
        if (conn->client_encoding < 0)
            conn->client_encoding = PG_SQL_ASCII;
        static_client_encoding = conn->client_encoding;
    }
    else if (strcmp(name, "standard_conforming_strings") == 0) {
        conn->std_strings = (strcmp(value, "on") == 0);
        static_std_strings = conn->std_strings;
    }
    else if (strcmp(name, "server_version") == 0) {
        // Parse version string into numeric form
        int vmaj, vmin, vrev;
        int cnt = sscanf(value, "%d.%d.%d", &vmaj, &vmin, &vrev);

        if (cnt == 3) {
            conn->sversion = (100 * vmaj + vmin) * 100 + vrev;  // old format: 9.6.1
        } else if (cnt == 2) {
            if (vmaj >= 10)
                conn->sversion = 100 * 100 * vmaj + vmin;  // new format: 10.1
            else
                conn->sversion = (100 * vmaj + vmin) * 100;  // old format: 9.6
        } else if (cnt == 1) {
            conn->sversion = 100 * 100 * vmaj;  // new format: 10
        } else {
            conn->sversion = 0;
        }
    }
    else if (strcmp(name, "default_transaction_read_only") == 0) {
        conn->default_transaction_read_only = (strcmp(value, "on") == 0) ? PG_BOOL_YES : PG_BOOL_NO;
    }
    else if (strcmp(name, "in_hot_standby") == 0) {
        conn->in_hot_standby = (strcmp(value, "on") == 0) ? PG_BOOL_YES : PG_BOOL_NO;
    }
    else if (strcmp(name, "scram_iterations") == 0) {
        conn->scram_sha_256_iterations = atoi(value);
    }
}
```