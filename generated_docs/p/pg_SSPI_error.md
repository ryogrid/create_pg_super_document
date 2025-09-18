# pg_SSPI_error

## Location
src/backend/libpq/auth.c: 1188 - 1205

## Overview
A utility function that generates formatted error messages for SSPI (Security Support Provider Interface) authentication failures in PostgreSQL.

## Definition


## Detailed Description
The pg_SSPI_error function is responsible for creating detailed error reports when SSPI authentication operations fail. It takes a Windows SECURITY_STATUS error code and converts it into a human-readable error message using the Windows FormatMessage API. The function provides two levels of error detail depending on whether the system error message can be successfully retrieved.

When FormatMessage succeeds in translating the SECURITY_STATUS code into a system message, the function reports both the custom error message and the detailed system message with the error code. If FormatMessage fails, it falls back to reporting just the custom message along with the raw hexadecimal error code.

## Parameters / Member Variables
- : The PostgreSQL error severity level (e.g., ERROR, WARNING) to be used when reporting the error
- : A custom error message string that should be translatable (caller should apply _() function)  
- : The SECURITY_STATUS error code returned from SSPI operations that needs to be interpreted

## Dependencies
- Functions called/Symbols referenced:
  - FormatMessage (Windows API)
  - ereport (PostgreSQL error reporting)
  - errmsg_internal
  - errdetail_internal
- Called from (representative examples):
  - pg_SSPI_recvauth
  - pg_SSPI_continue  
  - pg_SSPI_startup

## Notes and Other Information
- This function is specific to Windows builds of PostgreSQL as it uses Windows SSPI authentication
- The function is marked as static, indicating it's only used within the auth.c file
- Error messages are generated using errmsg_internal rather than errmsg, suggesting these are internal system errors not meant for direct user consumption
- The caller is responsible for making the errmsg parameter translatable by applying the _() macro
- Uses a fixed 256-byte buffer for system messages, which should be sufficient for most Windows error messages