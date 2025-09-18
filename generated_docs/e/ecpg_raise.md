# ecpg_raise

## Location
src/interfaces/ecpg/ecpglib/error.c: 13 - 218

## Overview
The primary error handling function in PostgreSQL's ECPG (Embedded SQL in C) library that sets SQL error codes and creates formatted error messages in the sqlca structure.

## Definition


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
  - ECPGfree_auto_mem
  - ecpg_gettext (for internationalization)
  - snprintf (for string formatting)
  - strlen
- Called from (representative examples):
  - [ECPGconnect](../E/ECPGconnect.md)
  - ecpg_get_data
  - [ecpg_check_PQresult](ecpg_check_PQresult.md)
  - ecpg_store_result
  - ecpg_process_output

## Notes and Other Information
- Sets sqlca->sqlcode to the provided error code
- Copies the sqlstate into sqlca->sqlstate (truncated to fit if necessary)
- Generates localized error messages using ecpg_gettext for internationalization
- Automatically calls ECPGfree_auto_mem() to clean up allocated memory
- Error messages are truncated at 149 characters to fit in sqlca->sqlerrm.sqlerrmc
- Logs the error using ecpg_log for debugging purposes
- Handles over 20 different ECPG error codes with specific error messages for each