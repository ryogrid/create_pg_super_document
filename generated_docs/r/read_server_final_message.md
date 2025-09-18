# read_server_final_message

## Location
[src/interfaces/libpq/fe-auth-scram.c:688-760](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth-scram.c#L688-L760)

## Overview
Parses and validates the final message from the PostgreSQL server in SCRAM authentication, extracting the server signature for mutual authentication verification.

## Definition
static bool read_server_final_message(fe_scram_state *state, char *input)

## Detailed Description
This function processes the server-final-message, which is the last message in the SCRAM authentication exchange. It handles two possible message types: error messages (starting with 'e') that indicate authentication failure, or success messages containing the server signature (attribute 'v'). The server signature proves that the server knows the correct password-derived keys, providing mutual authentication. The function decodes the base64-encoded server signature and stores it in the authentication state for verification against the expected signature calculated by the client.

## Parameters / Member Variables
- : Pointer to fe_scram_state structure to store the server signature
- : The raw server-final-message string received from the PostgreSQL server

## Dependencies
- Functions called/Symbols referenced:
  - [read_attr_value](read_attr_value.md) (parses SCRAM message attributes)
  - pg_b64_decode (decodes base64-encoded server signature)
  - pg_b64_dec_len (calculates decoded length)
  - strdup (duplicates message string)
  - malloc/free (memory management)
  - memcpy (copies signature data)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (error reporting)
- Called from:
  - [scram_exchange](../s/scram_exchange.md) (main SCRAM authentication handler)

## Notes and Other Information
- Handles both success and error responses from the server
- Server signature length must match the key length for the SCRAM mechanism
- Stores server_final_message for potential debugging or logging purposes
- Critical for mutual authentication - prevents malicious servers from impersonating legitimate ones
- Returns false if message is malformed or contains authentication errors
- The server signature will be compared against client-calculated expected signature
- Memory is allocated temporarily for decoding but freed before function returns