# pg_GSS_error

## Location
src/interfaces/libpq/fe-gssapi-common.c: 47 - 60

## Overview
Public function that reports GSS-API errors by combining major and minor status codes into a comprehensive error message.

## Definition
```c
void pg_GSS_error(const char *errmsg, OM_uint32 maj_stat, OM_uint32 min_stat)
```

## Detailed Description
This function provides a standardized way to report GSS-API errors throughout PostgreSQL. It takes a primary error message along with GSS-API major and minor status codes, then formats them into a detailed error report. The function uses `pg_GSS_error_int` to extract human-readable descriptions for both the major status (general GSS errors) and minor status (mechanism-specific errors).

The error is always reported at COMMERROR level to prevent infinite recursion that could occur if the error were sent to the client and triggered further GSS-API operations. The function uses fixed-size buffers (128 bytes each) to avoid memory allocation issues during error reporting.

## Parameters / Member Variables
- `errmsg`: Already-translated primary error message string
- `maj_stat`: GSS-API major status code containing general error information
- `min_stat`: GSS-API minor status code containing mechanism-specific error details

## Dependencies
- Functions called/Symbols referenced:
  - [pg_GSS_error_int](pg_GSS_error_int.md) (called twice for major and minor status)
  - ereport (PostgreSQL error reporting)
  - [errmsg_internal](../e/errmsg_internal.md) (PostgreSQL error message function)
  - [errdetail_internal](../e/errdetail_internal.md) (PostgreSQL error detail function)
  - COMMERROR (PostgreSQL constant)
- Called from (representative examples):
  - [pg_GSS_recvauth](pg_GSS_recvauth.md) (authentication)
  - [pg_GSS_checkauth](pg_GSS_checkauth.md) (authentication)
  - be_gssapi_write (secure communication)
  - [be_gssapi_read](../b/be_gssapi_read.md) (secure communication)
  - [pg_GSS_continue](pg_GSS_continue.md) (client-side authentication)
  - [pqsecure_open_gss](pqsecure_open_gss.md) (client-side connection)

## Notes and Other Information
- This function is used in both backend and frontend code (libpq)
- The 128-byte buffer limit per status type should accommodate all known GSS mechanisms
- Error reporting is done at COMMERROR level to prevent client communication loops
- The function assumes the primary error message is already translated
- Both backend and frontend versions exist with identical functionality
- Memory allocation is avoided during error reporting for reliability