# load_dh_file

## Location
[src/backend/libpq/be-secure-openssl.c:1027-1093](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1027-L1093)

## Overview
Loads and validates precomputed Diffie-Hellman (DH) parameters from a file to prevent downgrade attacks in SSL/TLS connections.

## Definition

```c
static DH  *
load_dh_file(char *filename, bool isServerStart)
```
## Detailed Description
This function loads DH parameters from a PEM-formatted file and performs comprehensive validation to ensure the parameters are cryptographically sound. It implements security measures to prevent "downgrade" attacks by verifying that the DBA-generated DH parameters file contains expected and secure values. The function performs multiple checks including prime validation, generator suitability, and safe prime verification.

The function handles errors differently based on the server state - during server startup, invalid parameters cause a FATAL error, while during runtime they generate LOG messages.

## Parameters / Member Variables
- : Path to the DH parameters file in PEM format
- : Boolean flag indicating if called during server startup (affects error severity)

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateFile](../A/AllocateFile.md) (opens the DH parameters file)
  - [FreeFile](../F/FreeFile.md) (closes the file handle)
  - [SSLerrmessage](../S/SSLerrmessage.md) (formats SSL error messages)
  - PEM_read_DHparams (OpenSSL function to read DH parameters)
  - DH_check (OpenSSL function to validate DH parameters)
  - DH_free (OpenSSL function to free DH structure)
- Called from (representative examples):
  - [initialize_dh](../i/initialize_dh.md) (src/backend/libpq/be-secure-openssl.c:1382)

## Notes and Other Information
- Returns NULL if file cannot be opened, read, or contains invalid parameters
- Performs multiple cryptographic validations including:
  - Prime number verification (DH_CHECK_P_NOT_PRIME)
  - Generator suitability check (DH_NOT_SUITABLE_GENERATOR)
  - Safe prime verification (DH_CHECK_P_NOT_SAFE_PRIME)
- Error severity depends on  parameter - FATAL during startup, LOG during runtime
- File absence is not treated as an error condition
- Part of PostgreSQL's SSL/TLS security infrastructure for secure connections

## Simplified Source

```c
// Simplified version of load_dh_file
static DH *load_dh_file(char *filename, bool isServerStart) {
    FILE *fp;
    DH *dh = NULL;
    int codes;

    // Step 1: Open the DH parameters file
    fp = AllocateFile(filename, "r");
    if (fp == NULL) {
        // Report error - severity depends on server startup state
        ereport(isServerStart ? FATAL : LOG,
                (errcode_for_file_access(),
                 errmsg("could not open DH parameters file \"%s\": %m", filename)));
        return NULL;
    }

    // Step 2: Read DH parameters from PEM format file
    dh = PEM_read_DHparams(fp, NULL, NULL, NULL);
    FreeFile(fp);

    if (dh == NULL) {
        ereport(isServerStart ? FATAL : LOG,
                (errcode(ERRCODE_CONFIG_FILE_ERROR),
                 errmsg("could not load DH parameters file: %s",
                        SSLerrmessage(ERR_get_error()))));
        return NULL;
    }

    // Step 3: Validate DH parameters for security
    if (DH_check(dh, &codes) == 0) {
        ereport(isServerStart ? FATAL : LOG,
                (errcode(ERRCODE_CONFIG_FILE_ERROR),
                 errmsg("invalid DH parameters: %s",
                        SSLerrmessage(ERR_get_error()))));
        DH_free(dh);
        return NULL;
    }

    // Step 4: Check specific cryptographic requirements
    if (codes & DH_CHECK_P_NOT_PRIME) {
        ereport(isServerStart ? FATAL : LOG,
                (errcode(ERRCODE_CONFIG_FILE_ERROR),
                 errmsg("invalid DH parameters: p is not prime")));
        DH_free(dh);
        return NULL;
    }

    if ((codes & DH_NOT_SUITABLE_GENERATOR) && (codes & DH_CHECK_P_NOT_SAFE_PRIME)) {
        ereport(isServerStart ? FATAL : LOG,
                (errcode(ERRCODE_CONFIG_FILE_ERROR),
                 errmsg("invalid DH parameters: neither suitable generator or safe prime")));
        DH_free(dh);
        return NULL;
    }

    // Step 5: Return validated DH parameters
    return dh;
}
```

Key simplifications made:
- Added step-by-step comments to clarify the process flow
- Grouped logical operations together for better readability
- Maintained all error handling as it's critical for security
- Preserved the exact validation logic since DH parameter security is essential
- Kept the conditional error severity (FATAL vs LOG) as it's important behavior
- Maintained proper resource cleanup (DH_free) on error paths