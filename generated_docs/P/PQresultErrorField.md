# PQresultErrorField

## Location
src/interfaces/libpq/fe-exec.c: 3466 - 3480

## Overview
PQresultErrorField retrieves a specific error field from a PGresult by field code, providing access to individual components of PostgreSQL error messages.

## Definition
char *PQresultErrorField(const PGresult *res, int fieldcode)

## Detailed Description
This function searches through the error fields stored in a PGresult object and returns the contents of the field matching the specified field code. It iterates through a linked list of error message fields until it finds one with the matching code, then returns a pointer to the field's contents. The function is used to extract specific pieces of error information such as severity, SQLSTATE, primary message, detail, hint, position, and other standardized error components.

The returned pointer points to memory owned by the PGresult object and should not be freed by the caller. The data remains valid until the PGresult is destroyed.

## Parameters / Member Variables
- : Pointer to the PGresult containing error information
- : Integer code identifying the specific error field to retrieve (e.g., PG_DIAG_SEVERITY, PG_DIAG_SQLSTATE, PG_DIAG_MESSAGE_PRIMARY)

## Dependencies
- Functions called/Symbols referenced:
  - [PGMessageField](PGMessageField.md) (struct type for error field storage)
- Called from (representative examples):
  - [libpqrcv_exec](../l/libpqrcv_exec.md) (replication error handling)
  - [BaseBackup](../B/BaseBackup.md) (pg_basebackup error processing)
  - [minimal_error_message](../m/minimal_error_message.md) (psql error formatting)
  - [SetResultVariables](../S/SetResultVariables.md) (psql variable setting)
  - [pqBuildErrorMessage3](../p/pqBuildErrorMessage3.md) (internal error message construction)
  - [ecpg_raise_backend](../e/ecpg_raise_backend.md) (ECPG error handling)

## Notes and Other Information
- Returns NULL if the result is NULL or if the requested field code is not found
- The returned string pointer is owned by the PGresult and should not be freed
- Common field codes include PG_DIAG_SEVERITY, PG_DIAG_SQLSTATE, PG_DIAG_MESSAGE_PRIMARY, PG_DIAG_MESSAGE_DETAIL, PG_DIAG_MESSAGE_HINT
- Part of PostgreSQL's structured error reporting system
- Widely used throughout PostgreSQL client tools and applications for detailed error analysis
- The function performs a linear search through the error fields list