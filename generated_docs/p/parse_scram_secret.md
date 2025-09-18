# parse_scram_secret

## Location
src/backend/libpq/auth-scram.c: 589 - 682

## Overview
Parses and validates the format of a given SCRAM secret string, extracting authentication parameters including iteration count, salt, stored key, and server key.

## Definition


## Detailed Description
This function parses a SCRAM (Salted Challenge Response Authentication Mechanism) secret string formatted as:


The function validates each component of the secret:
- Verifies the scheme is "SCRAM-SHA-256"
- Extracts and validates the iteration count as a positive integer
- Decodes and validates the base64-encoded salt
- Decodes the base64-encoded stored key and server key, ensuring they match the expected key length

On successful parsing, all extracted values are returned via output parameters. The salt is returned as a base64-encoded null-terminated string that is palloc'd by this function.

## Parameters / Member Variables
- : Input SCRAM secret string to be parsed
- : Output parameter for the iteration count extracted from the secret
- : Output parameter set to the hash algorithm type (PG_SHA256)
- : Output parameter set to the key length (SCRAM_SHA_256_KEY_LEN)
- : Output parameter for the base64-encoded salt string (palloc'd)
- : Pre-allocated buffer (SCRAM_MAX_KEY_LEN) to receive the decoded stored key
- : Pre-allocated buffer (SCRAM_MAX_KEY_LEN) to receive the decoded server key

## Dependencies
- Functions called/Symbols referenced:
  - [pstrdup](pstrdup.md)
  - strtok
  - strcmp
  - strtol
  - pg_b64_dec_len
  - pg_b64_decode
  - [palloc](palloc.md)
  - memcpy
  - PG_SHA256
  - SCRAM_SHA_256_KEY_LEN
- Called from (representative examples):
  - [scram_init](../s/scram_init.md)
  - [scram_verify_plain_password](../s/scram_verify_plain_password.md)
  - [get_password_type](../g/get_password_type.md)

## Notes and Other Information
- Returns true on successful parsing, false on any validation failure
- Uses goto-based error handling with a single  label
- The caller is responsible for pre-allocating buffers for stored_key and server_key
- The salt output parameter is dynamically allocated and must be freed by the caller
- Currently only supports SCRAM-SHA-256 algorithm
- Part of PostgreSQL's SCRAM authentication implementation