# secure_loaded_verify_locations

## Location
src/backend/libpq/be-secure.c: 97 - 109

## Overview
Returns a boolean indicating whether PostgreSQL has successfully loaded root CA certificates for verifying SSL/TLS client certificates during authentication.

## Definition


## Detailed Description
The `secure_loaded_verify_locations` function provides a way to query whether the server has loaded trusted root Certificate Authority (CA) certificates that can be used to verify client certificates during SSL/TLS authentication. This is crucial for SSL certificate-based authentication mechanisms where the server needs to validate that client certificates are signed by a trusted CA.

When PostgreSQL is compiled with SSL support, it returns the value of the global variable `ssl_loaded_verify_locations`, which tracks whether CA certificates have been successfully loaded from configured locations (such as ssl_ca_file or ssl_ca_dir). Without SSL support, it always returns false since certificate verification is not available.

## Parameters / Member Variables
- None: This function takes no parameters and queries global SSL state.

## Dependencies
- Functions called/Symbols referenced:
  - ssl_loaded_verify_locations (global variable when USE_SSL is defined)
  - USE_SSL (compile-time macro check)
- Called from (representative examples):
  - ClientAuthentication (to determine if certificate-based authentication is possible)
  - FeBeWaitSetNEvents (referenced in libpq.h)

## Notes and Other Information
- Returns true only when SSL is compiled in AND CA certificates have been successfully loaded
- Essential for SSL certificate-based client authentication methods
- The actual CA loading occurs during SSL initialization via ssl_ca_file and ssl_ca_dir configuration parameters
- Used by authentication code to determine available authentication methods
- Always returns false when PostgreSQL is built without SSL support