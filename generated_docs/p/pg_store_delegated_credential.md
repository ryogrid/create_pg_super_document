# pg_store_delegated_credential

## Location
[src/backend/libpq/be-gssapi-common.c:104-147](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-gssapi-common.c#L104-L147)

## Overview
This function stores delegated GSS-API credentials received during authentication into a memory-based credential cache, making them available for subsequent operations within the current PostgreSQL backend process.

## Definition

```c
void
pg_store_delegated_credential(gss_cred_id_t cred)
```
## Detailed Description
The  function manages the storage of delegated GSS-API credentials in PostgreSQL's backend processes. When a client delegates credentials during GSS-API authentication, this function stores them in a memory-based credential cache (ccache) using the GSS-API  function.

The function specifically configures the credential storage to use a memory cache (), which ensures the credentials are only available to the current process and are automatically cleaned up when the process terminates. After successfully storing the credentials, it releases the original credential handle and sets the  environment variable to point to the memory cache, enabling later credential acquisition operations to find the stored delegated credentials.

## Parameters / Member Variables
- : The GSS-API credential handle containing the delegated credentials to be stored

## Dependencies
- Functions called/Symbols referenced:
  - gss_store_cred_into (GSS-API function for storing credentials into a specific cache)
  - gss_release_cred (GSS-API function for releasing credential handles)
  - [pg_GSS_error](pg_GSS_error.md) (PostgreSQL's GSS-API error reporting function)
  - setenv (standard C library function for setting environment variables)
  - GSS_MEMORY_CACHE (GSS-API constant specifying memory-based credential cache)
- Called from (representative examples):
  - [pg_GSS_recvauth](pg_GSS_recvauth.md) (during GSS-API authentication processing)
  - [secure_open_gssapi](../s/secure_open_gssapi.md) (during secure GSS-API connection establishment)

## Notes and Other Information
- Uses memory-based credential cache () for security - credentials are automatically cleaned up when the process exits
- Stores credentials with  usage, meaning they can be used for initiating connections (like libpq client connections)
- Sets the  flag to true, allowing replacement of existing credentials in the cache
- Sets the  flag to true, making these the default credentials for the process
- Sets the  environment variable to ensure later GSS-API operations can locate the stored credentials
- Error handling uses PostgreSQL's standard GSS-API error reporting mechanism
- This functionality is essential for credential delegation in PostgreSQL's GSS-API authentication system
- Only available in the backend - there's no equivalent frontend version of this function

## Simplified Source

```c
// Simplified version of pg_store_delegated_credential
void pg_store_delegated_credential(gss_cred_id_t cred) {
    OM_uint32 major, minor;
    gss_OID_set mech;
    gss_cred_usage_t usage;

    // Configure credential cache to use memory storage
    gss_key_value_element_desc cc;
    gss_key_value_set_desc ccset;
    cc.key = "ccache";
    cc.value = GSS_MEMORY_CACHE;
    ccset.count = 1;
    ccset.elements = &cc;

    // Store delegated credential in memory cache
    major = gss_store_cred_into(&minor,
                                cred,
                                GSS_C_INITIATE,      // for starting libpq connections
                                GSS_C_NULL_OID,      // store all mechanisms
                                true,                // overwrite existing
                                true,                // make default
                                &ccset,
                                &mech,
                                &usage);

    if (major != GSS_S_COMPLETE) {
        pg_GSS_error("gss_store_cred", major, minor);
    }

    // Release original credential handle since it's now stored
    major = gss_release_cred(&minor, &cred);
    if (major != GSS_S_COMPLETE) {
        pg_GSS_error("gss_release_cred", major, minor);
    }

    // Set environment variable for later credential lookup
    setenv("KRB5CCNAME", GSS_MEMORY_CACHE, 1);
}
```

Key simplifications made:
- Added explanatory comments for credential cache configuration
- Clarified purpose of each GSS-API parameter
- Maintained essential credential storage and cleanup logic
- Preserved error handling for both storage and release operations