# PQresultErrorMessage

## Location
src/interfaces/libpq/fe-exec.c: 3427 - 3434

## Overview
PQresultErrorMessage retrieves the error message associated with a PostgreSQL query result, providing detailed information about what went wrong during command execution.

## Definition


## Detailed Description
This function extracts the error message from a PGresult object, if one exists. It serves as an essential accessor function for error handling in libpq-based applications. The function provides a safe way to retrieve error details from failed or problematic database operations.

The function performs null-safety checks on both the result pointer and the error message field. If either the result is NULL or no error message is present (errMsg field is NULL), it returns an empty string rather than a NULL pointer, preventing potential segmentation faults in client code.

When an error occurs during query execution, PostgreSQL stores detailed diagnostic information in the result object's errMsg field. This function provides access to that information, allowing applications to display meaningful error messages to users or log detailed diagnostic information for debugging purposes.

## Parameters / Member Variables
- : Const pointer to a PGresult structure that may contain an error message

## Dependencies
- Functions called/Symbols referenced:
  - None (direct field access only)
- Called from:
  - Widely used across PostgreSQL tools and utilities
  - pg_createsubscriber.c (multiple functions for database setup operations)
  - pg_recvlogical.c (StreamLogicalLog)
  - receivelog.c (ReceiveXlogStream)
  - libpq_source.c (pg_rewind tool functions)
  - common.c (ExecQueryAndProcessResults in psql)
  - ecpg error handling functions
  - defaultNoticeReceiver (fe-connect.c:7366)
  - Various test modules for error reporting

## Notes and Other Information
- This is a public libpq API function available to all client applications
- Always returns a valid string pointer, never NULL (returns empty string for NULL inputs)
- The returned string should not be modified or freed by the caller
- Essential for proper error handling and user feedback in PostgreSQL client applications
- The error message may contain multiple lines and detailed diagnostic information
- Thread-safe as it only reads from the result structure
- Commonly used in conjunction with PQresultStatus to provide comprehensive error reporting
- The function is located at src/interfaces/libpq/fe-exec.c:3427-3434