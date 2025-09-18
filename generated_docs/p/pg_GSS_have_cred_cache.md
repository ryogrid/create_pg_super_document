# pg_GSS_have_cred_cache

## Location
src/interfaces/libpq/fe-gssapi-common.c: 61 - 81

## Overview
Client-side function that checks if GSS-API credentials can be acquired from the credential cache and optionally returns the credential handle.

## Definition
```c
bool pg_GSS_have_cred_cache(gss_cred_id_t *cred_out)
```

## Detailed Description
This function attempts to acquire GSS-API credentials using the default credential cache. It uses `gss_acquire_cred` with default parameters to check if valid credentials are available for initiating GSS-API security contexts. The function is primarily used to determine whether GSS-API authentication can proceed without prompting the user for credentials.

If credentials are successfully acquired, they are returned through the output parameter and can be used for subsequent GSS-API operations. If no credentials are available or the acquisition fails, the function returns false and sets the output parameter to NULL.

## Parameters / Member Variables
- `cred_out`: Output parameter that receives the acquired credential handle on success, or NULL on failure

## Dependencies
- Functions called/Symbols referenced:
  - gss_acquire_cred (GSS-API function)
  - GSS_C_NO_CREDENTIAL (GSS-API constant)
  - GSS_C_NO_NAME (GSS-API constant)
  - GSS_C_NO_OID_SET (GSS-API constant)
  - GSS_C_INITIATE (GSS-API constant)
  - GSS_S_COMPLETE (GSS-API constant)
- Called from (representative examples):
  - pg_GSS_continue (client authentication)
  - CONNECTION_FAILED (connection handling)
  - SELECT_NEXT_METHOD (authentication method selection)
  - pqsecure_open_gss (secure connection establishment)

## Notes and Other Information
- This is a client-side only function (libpq)
- The function uses default parameters for credential acquisition (no specific name, no time limit, all mechanisms)
- Acquired credentials should be properly released using `gss_release_cred` when no longer needed
- The function is used to probe credential availability before attempting authentication
- Returns true only if credentials are successfully acquired, false for any failure condition
- Used in connection logic to determine if GSS authentication is viable