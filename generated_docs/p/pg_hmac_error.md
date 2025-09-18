# pg_hmac_error

## Location
src/common/hmac_openssl.c: 354 - 383

## Overview
Returns a human-readable error message describing the last error that occurred in a HMAC context.

## Definition
```c
const char *pg_hmac_error(pg_hmac_ctx *ctx)
```

## Detailed Description
The pg_hmac_error function provides detailed error information for debugging and logging purposes when HMAC operations fail. It first checks if a specific error reason string is available (from underlying cryptographic functions), and if not, it translates the error code into a localized error message. The function handles various error conditions including internal errors, out-of-memory conditions, and success states. All returned strings are localized using the gettext mechanism.

## Parameters / Member Variables
- `ctx`: Pointer to the HMAC context structure (can be NULL, treated as out-of-memory)

## Dependencies
- Functions called/Symbols referenced:
  - PG_HMAC_ERROR_NONE
  - PG_HMAC_ERROR_INTERNAL  
  - PG_HMAC_ERROR_OOM
  - Assert (debugging assertion)
  - _ (gettext localization macro)
- Called from (representative examples):
  - verify_client_proof (SCRAM authentication error handling)
  - build_server_final_message (SCRAM authentication error handling)
  - scram_SaltedPassword (SCRAM key derivation error handling)
  - calculate_client_proof (libpq SCRAM client error handling)

## Notes and Other Information
- Returns localized error messages using gettext
- Prioritizes ctx->errreason over ctx->error for more specific error details
- Safe to call with NULL context (returns "out of memory" message)
- Used for error reporting and debugging throughout SCRAM authentication
- All error messages are translated for internationalization support
- Includes assertion to catch unexpected error states during development