# ecpg_raise

## Location
[src/interfaces/ecpg/ecpglib/error.c:13-218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/error.c#L13-L218)

## Overview
The primary error handling function in PostgreSQL's ECPG (Embedded SQL in C) library that sets SQL error codes and creates formatted error messages in the sqlca structure.

## Definition

```c
struct sqlca_t *sqlca = ECPGget_sqlca();
```
## Detailed Description
The  function is the central error reporting mechanism for the ECPG library. It populates the SQL Communication Area (sqlca) structure with error information and provides localized error messages for various ECPG error conditions. The function handles a comprehensive set of error codes ranging from data type conversion errors to connection failures, formatting appropriate error messages for each case. After setting the error information, it automatically frees any allocated memory to prevent memory leaks during error conditions.

## Parameters / Member Variables
- : The line number in the source code where the error occurred
- : The ECPG error code (e.g., ECPG_NOT_FOUND, ECPG_OUT_OF_MEMORY)  
- : The SQL state string conforming to SQL standard error codes
- : Optional additional error context string (may be NULL for some error types)

## Dependencies
- Functions called/Symbols referenced:
  - ECPGget_sqlca
  - [ecpg_log](ecpg_log.md)
  - [ECPGfree_auto_mem](../E/ECPGfree_auto_mem.md)
  - [ecpg_gettext](ecpg_gettext.md) (for internationalization)
  - snprintf (for string formatting)
  - strlen
- Called from (representative examples):
  - [ECPGconnect](../E/ECPGconnect.md)
  - [ecpg_get_data](ecpg_get_data.md)
  - [ecpg_check_PQresult](ecpg_check_PQresult.md)
  - [ecpg_store_result](ecpg_store_result.md)
  - [ecpg_process_output](ecpg_process_output.md)

## Notes and Other Information
- Sets sqlca->sqlcode to the provided error code
- Copies the sqlstate into sqlca->sqlstate (truncated to fit if necessary)
- Generates localized error messages using ecpg_gettext for internationalization
- Automatically calls ECPGfree_auto_mem() to clean up allocated memory
- Error messages are truncated at 149 characters to fit in sqlca->sqlerrm.sqlerrmc
- Logs the error using ecpg_log for debugging purposes
- Handles over 20 different ECPG error codes with specific error messages for each

## Simplified Source

```c
void ecpg_raise(int line, int code, const char *sqlstate, const char *str) {
    struct sqlca_t *sqlca = ECPGget_sqlca();

    // Handle memory allocation failure
    if (!sqlca) {
        ecpg_log("out of memory");
        ECPGfree_auto_mem();
        return;
    }

    // Set error code and state
    sqlca->sqlcode = code;
    strncpy(sqlca->sqlstate, sqlstate, sizeof(sqlca->sqlstate));

    // Generate appropriate error message based on error code
    switch (code) {
        case ECPG_NOT_FOUND:
            snprintf(sqlca->sqlerrm.sqlerrmc, sizeof(sqlca->sqlerrm.sqlerrmc),
                     ecpg_gettext("no data found on line %d"), line);
            break;

        case ECPG_OUT_OF_MEMORY:
            snprintf(sqlca->sqlerrm.sqlerrmc, sizeof(sqlca->sqlerrm.sqlerrmc),
                     ecpg_gettext("out of memory on line %d"), line);
            break;

        case ECPG_UNSUPPORTED:
            snprintf(sqlca->sqlerrm.sqlerrmc, sizeof(sqlca->sqlerrm.sqlerrmc),
                     ecpg_gettext("unsupported type \"%s\" on line %d"), str, line);
            break;

        case ECPG_CONVERT_BOOL:
            if (str)
                snprintf(sqlca->sqlerrm.sqlerrmc, sizeof(sqlca->sqlerrm.sqlerrmc),
                         ecpg_gettext("invalid syntax for type boolean: \"%s\", on line %d"), str, line);
            else
                snprintf(sqlca->sqlerrm.sqlerrmc, sizeof(sqlca->sqlerrm.sqlerrmc),
                         ecpg_gettext("could not convert boolean value: size mismatch, on line %d"), line);
            break;

        // ... other error cases follow same pattern ...

        default:
            snprintf(sqlca->sqlerrm.sqlerrmc, sizeof(sqlca->sqlerrm.sqlerrmc),
                     ecpg_gettext("SQL error %d on line %d"), code, line);
    }

    // Set error message length and log the error
    sqlca->sqlerrm.sqlerrml = strlen(sqlca->sqlerrm.sqlerrmc);
    ecpg_log("raising sqlcode %d on line %d: %s\n", code, line, sqlca->sqlerrm.sqlerrmc);

    // Clean up allocated memory
    ECPGfree_auto_mem();
}
```