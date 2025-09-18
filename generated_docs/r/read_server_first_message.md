# read_server_first_message

## Location
src/interfaces/libpq/fe-auth-scram.c: 602 - 687

## Overview
Parses and validates the first message received from the PostgreSQL server during SCRAM authentication, extracting the server nonce, salt, and iteration count.

## Definition
static bool read_server_first_message(fe_scram_state *state, char *input)

## Detailed Description
This function processes the server-first-message in the SCRAM authentication protocol. It parses the message to extract three critical components: the server nonce (which must contain the client nonce as a prefix), the base64-encoded salt used for key derivation, and the iteration count for PBKDF2. The function performs validation to ensure the server properly incorporated the client nonce and that all parameters are well-formed. This data is essential for the subsequent cryptographic operations in the SCRAM exchange.

## Parameters / Member Variables
- : Pointer to fe_scram_state structure to store extracted authentication parameters
- : The raw server-first-message string received from the PostgreSQL server

## Dependencies
- Functions called/Symbols referenced:
  - read_attr_value (parses SCRAM message attributes)
  - pg_b64_decode (decodes base64-encoded salt)
  - pg_b64_dec_len (calculates decoded length)
  - strdup (duplicates strings)
  - malloc (allocates memory for salt)
  - strtol (converts iteration count string to integer)
  - libpq_append_conn_error (error reporting)
- Called from:
  - scram_exchange (main SCRAM authentication handler)

## Notes and Other Information
- Validates that server nonce contains client nonce as prefix (prevents replay attacks)
- Decodes base64-encoded salt into binary format for cryptographic operations
- Ensures iteration count is a positive integer (security requirement)
- Stores server_first_message for later use in authentication verification
- Returns false on any parsing error or validation failure
- Critical security validation - improper nonce handling could allow attacks
- Memory allocated for salt and nonce must be freed when authentication completes