# get_role_password

## Location
src/backend/libpq/crypt.c: 36 - 87

## Overview
Fetches the stored password for a user from the pg_authid system catalog for authentication purposes, with additional validation for password expiration.

## Definition


## Detailed Description
This function retrieves a user's password from the PostgreSQL system catalog  and performs several validation checks. It searches for the specified role in the system cache and extracts both the password and password expiration date. The function also validates that the password has not expired by comparing the  timestamp with the current time. If any error occurs (role doesn't exist, no password assigned, or password expired), the function returns NULL and provides a detailed error message for logging purposes. The error details are specifically designed not to be sent to the client to avoid exposing sensitive user information.

## Parameters / Member Variables
- : The name of the role/user whose password is being retrieved
- : Output parameter that receives a palloc'd string describing any error that occurred, intended for postmaster logging (not client-facing)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (to find the role in pg_authid)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md) (to extract rolpassword and rolvaliduntil attributes)
  - TextDatumGetCString (to convert password datum to C string)
  - DatumGetTimestampTz (to extract timestamp from rolvaliduntil)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md) (to get current time for expiration check)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (to release the system cache tuple)
- Called from (representative examples):
  - [CheckPasswordAuth](../C/CheckPasswordAuth.md) (in src/backend/libpq/auth.c)
  - [CheckPWChallengeAuth](../C/CheckPWChallengeAuth.md) (in src/backend/libpq/auth.c)

## Notes and Other Information
- Returns NULL on any error condition (role not found, no password, expired password)
- Error messages are logged but not sent to clients for security reasons
- Uses system cache for efficient role lookup via AUTHNAME
- Handles NULL values appropriately for optional fields like rolvaliduntil
- Part of PostgreSQL's authentication infrastructure in the libpq backend
- Memory for the returned password string is allocated and must be freed by the caller