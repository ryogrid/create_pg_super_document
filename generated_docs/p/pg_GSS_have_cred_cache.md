# pg_GSS_have_cred_cache

## Location
[src/interfaces/libpq/fe-gssapi-common.c:61-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-gssapi-common.c#L61-L81)

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
  - [pg_GSS_continue](pg_GSS_continue.md) (client authentication)
  - CONNECTION_FAILED (connection handling)
  - SELECT_NEXT_METHOD (authentication method selection)
  - [pqsecure_open_gss](pqsecure_open_gss.md) (secure connection establishment)

## Notes and Other Information
- This is a client-side only function (libpq)
- The function uses default parameters for credential acquisition (no specific name, no time limit, all mechanisms)
- Acquired credentials should be properly released using `gss_release_cred` when no longer needed
- The function is used to probe credential availability before attempting authentication
- Returns true only if credentials are successfully acquired, false for any failure condition
- Used in connection logic to determine if GSS authentication is viable

## Simplified Source

```c
// Simplified version of pg_GSS_have_cred_cache
bool pg_GSS_have_cred_cache(gss_cred_id_t *cred_out) {
    OM_uint32 major, minor;
    gss_cred_id_t cred = GSS_C_NO_CREDENTIAL;

    // Try to acquire default GSS credentials for initiating connections
    major = gss_acquire_cred(&minor, GSS_C_NO_NAME, 0, GSS_C_NO_OID_SET,
                            GSS_C_INITIATE, &cred, NULL, NULL);

    // Check if credential acquisition was successful
    if (major != GSS_S_COMPLETE) {
        *cred_out = NULL;
        return false;
    }

    // Return the acquired credentials
    *cred_out = cred;
    return true;
}
```

Key simplifications made:
- Added clear comments explaining the credential acquisition process
- Emphasized the role of checking for default credentials
- Made the success/failure logic more explicit
- Clarified the output parameter handling
- Focused on the core functionality of credential probing