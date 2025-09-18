# load_dh_file

## Location
src/backend/libpq/be-secure-openssl.c: 1027 - 1093

## Overview
Loads and validates precomputed Diffie-Hellman (DH) parameters from a file to prevent downgrade attacks in SSL/TLS connections.

## Definition


## Detailed Description
This function loads DH parameters from a PEM-formatted file and performs comprehensive validation to ensure the parameters are cryptographically sound. It implements security measures to prevent "downgrade" attacks by verifying that the DBA-generated DH parameters file contains expected and secure values. The function performs multiple checks including prime validation, generator suitability, and safe prime verification.

The function handles errors differently based on the server state - during server startup, invalid parameters cause a FATAL error, while during runtime they generate LOG messages.

## Parameters / Member Variables
- : Path to the DH parameters file in PEM format
- : Boolean flag indicating if called during server startup (affects error severity)

## Dependencies
- Functions called/Symbols referenced:
  - AllocateFile (opens the DH parameters file)
  - FreeFile (closes the file handle)
  - SSLerrmessage (formats SSL error messages)
  - PEM_read_DHparams (OpenSSL function to read DH parameters)
  - DH_check (OpenSSL function to validate DH parameters)
  - DH_free (OpenSSL function to free DH structure)
- Called from (representative examples):
  - initialize_dh (src/backend/libpq/be-secure-openssl.c:1382)

## Notes and Other Information
- Returns NULL if file cannot be opened, read, or contains invalid parameters
- Performs multiple cryptographic validations including:
  - Prime number verification (DH_CHECK_P_NOT_PRIME)
  - Generator suitability check (DH_NOT_SUITABLE_GENERATOR)
  - Safe prime verification (DH_CHECK_P_NOT_SAFE_PRIME)
- Error severity depends on  parameter - FATAL during startup, LOG during runtime
- File absence is not treated as an error condition
- Part of PostgreSQL's SSL/TLS security infrastructure for secure connections