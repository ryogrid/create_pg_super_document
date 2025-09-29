# ecpg_raise_backend

## Location
[src/interfaces/ecpg/ecpglib/error.c:219-280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/error.c#L219-L280)

## Overview
Handles PostgreSQL backend error reporting in ECPG by extracting error information from PGresult and PGconn objects and populating the sqlca structure with backend-specific error details.

## Definition

```c
struct sqlca_t *sqlca = ECPGget_sqlca();
```
## Detailed Description
The  function processes errors that originate from the PostgreSQL backend server. It extracts error information from libpq structures (PGresult and PGconn), maps PostgreSQL error codes to ECPG-specific codes, and handles backward compatibility concerns. The function intelligently handles cases where error information might be incomplete or connection-related, providing appropriate fallback error messages. It also includes special handling for connection failures and maps specific SQL states to corresponding ECPG error codes based on compatibility mode settings.

## Parameters / Member Variables
- : The line number in the source code where the error occurred
- : PGresult object containing the query result and potential error information
- : PGconn object representing the database connection
- : Compatibility mode flag (affects error code mapping for Informix compatibility)

## Dependencies
- Functions called/Symbols referenced:
  - ECPGget_sqlca
  - [ecpg_log](ecpg_log.md)
  - [ECPGfree_auto_mem](../E/ECPGfree_auto_mem.md)
  - [PQresultErrorField](../P/PQresultErrorField.md) (to extract error fields from result)
  - [PQerrorMessage](../P/PQerrorMessage.md) (fallback for connection errors)
  - [PQstatus](../P/PQstatus.md) (to check connection status)
  - [ecpg_gettext](ecpg_gettext.md) (for localized messages)
  - snprintf, strlen, strncpy, strncmp
- Called from (representative examples):
  - [ecpg_check_PQresult](ecpg_check_PQresult.md)
  - [ecpg_process_output](ecpg_process_output.md)

## Notes and Other Information
- Extracts SQLSTATE using PG_DIAG_SQLSTATE and primary error message using PG_DIAG_MESSAGE_PRIMARY
- Falls back to PQerrorMessage() when PQresultErrorField() returns NULL
- Special handling for connection failures (CONNECTION_BAD status) with appropriate SQLSTATE
- Maps specific SQL states to ECPG error codes (e.g., '23505' for duplicate key, '21000' for subselect errors)
- Supports Informix compatibility mode with different error code mappings
- Uses ECPG_PGSQL as default sqlcode for unmapped PostgreSQL errors
- Automatically frees allocated memory after setting error information
- Error messages are formatted to include the line number where the error occurred

## Simplified Source

```c
void ecpg_raise_backend(int line, PGresult *result, PGconn *conn, int compat) {
    struct sqlca_t *sqlca = ECPGget_sqlca();
    char *sqlstate;
    char *message;

    // Check if sqlca is available
    if (sqlca == NULL) {
        ecpg_log("out of memory");
        ECPGfree_auto_mem();
        return;
    }

    // Extract error state and message from result
    sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
    if (sqlstate == NULL)
        sqlstate = ECPG_SQLSTATE_ECPG_INTERNAL_ERROR;

    message = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);
    if (message == NULL)
        message = PQerrorMessage(conn);

    // Handle special case: connection failure
    if (strcmp(sqlstate, ECPG_SQLSTATE_ECPG_INTERNAL_ERROR) == 0) {
        if (PQstatus(conn) == CONNECTION_BAD) {
            sqlstate = "57P02";
            message = ecpg_gettext("the connection to the server was lost");
        }
    }

    // Populate sqlca structure with error information
    snprintf(sqlca->sqlerrm.sqlerrmc, sizeof(sqlca->sqlerrm.sqlerrmc),
             "%s on line %d", message, line);
    sqlca->sqlerrm.sqlerrml = strlen(sqlca->sqlerrm.sqlerrmc);
    strncpy(sqlca->sqlstate, sqlstate, sizeof(sqlca->sqlstate));

    // Map SQL states to ECPG error codes for compatibility
    if (strncmp(sqlca->sqlstate, "23505", sizeof(sqlca->sqlstate)) == 0)
        sqlca->sqlcode = INFORMIX_MODE(compat) ? ECPG_INFORMIX_DUPLICATE_KEY : ECPG_DUPLICATE_KEY;
    else if (strncmp(sqlca->sqlstate, "21000", sizeof(sqlca->sqlstate)) == 0)
        sqlca->sqlcode = INFORMIX_MODE(compat) ? ECPG_INFORMIX_SUBSELECT_NOT_ONE : ECPG_SUBSELECT_NOT_ONE;
    else
        sqlca->sqlcode = ECPG_PGSQL;

    // Log the error and clean up
    ecpg_log("raising sqlstate %.*s (sqlcode %ld): %s\n",
             (int) sizeof(sqlca->sqlstate), sqlca->sqlstate, sqlca->sqlcode,
             sqlca->sqlerrm.sqlerrmc);
    ECPGfree_auto_mem();
}
```