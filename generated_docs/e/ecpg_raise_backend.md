# ecpg_raise_backend

## Location
src/interfaces/ecpg/ecpglib/error.c: 219 - 280

## Overview
Handles PostgreSQL backend error reporting in ECPG by extracting error information from PGresult and PGconn objects and populating the sqlca structure with backend-specific error details.

## Definition


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
  - ECPGfree_auto_mem
  - [PQresultErrorField](../P/PQresultErrorField.md) (to extract error fields from result)
  - [PQerrorMessage](../P/PQerrorMessage.md) (fallback for connection errors)
  - PQstatus (to check connection status)
  - ecpg_gettext (for localized messages)
  - snprintf, strlen, strncpy, strncmp
- Called from (representative examples):
  - [ecpg_check_PQresult](ecpg_check_PQresult.md)
  - ecpg_process_output

## Notes and Other Information
- Extracts SQLSTATE using PG_DIAG_SQLSTATE and primary error message using PG_DIAG_MESSAGE_PRIMARY
- Falls back to PQerrorMessage() when PQresultErrorField() returns NULL
- Special handling for connection failures (CONNECTION_BAD status) with appropriate SQLSTATE
- Maps specific SQL states to ECPG error codes (e.g., '23505' for duplicate key, '21000' for subselect errors)
- Supports Informix compatibility mode with different error code mappings
- Uses ECPG_PGSQL as default sqlcode for unmapped PostgreSQL errors
- Automatically frees allocated memory after setting error information
- Error messages are formatted to include the line number where the error occurred