# to_regproc

## Location
src/backend/utils/adt/regproc.c: 118 - 135

## Overview
A wrapper function that converts a procedure name text to a RegProcedure OID, returning NULL if the procedure is not found instead of throwing an error.

## Definition


## Detailed Description
The to_regproc function serves as a safe wrapper around regprocin, providing NULL-on-error semantics instead of error throwing. It accepts a text input (rather than a C-string like regprocin) and converts it to the corresponding procedure's OID. This function is typically used in contexts where missing procedures should be handled gracefully rather than causing query failure.

The function uses PostgreSQL's error save context mechanism to catch errors from the underlying regprocin call and convert them to NULL returns, making it suitable for use in SQL queries where error handling is preferred over exceptions.

## Parameters / Member Variables
- : Input text value containing the procedure name to be converted to OID

## Dependencies
- Functions called/Symbols referenced:
  - : Converts PostgreSQL text type to C-string
  - : Underlying function that performs the actual name-to-OID conversion
  - : Safe function call wrapper that captures errors
  - : Error context structure for capturing conversion errors
  - : Returns the converted OID or NULL
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- Error handling: Uses ErrorSaveContext to convert regprocin errors into NULL returns, providing graceful failure semantics
- Input format: Accepts PostgreSQL text type input, making it suitable for direct use in SQL
- Safety wrapper: Provides a non-throwing alternative to regprocin for applications requiring NULL-on-error behavior
- Function registration: Likely registered as a SQL-callable function for use in queries and expressions