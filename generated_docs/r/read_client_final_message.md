# read_client_final_message

## Location
[src/backend/libpq/auth-scram.c:1253-1398](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L1253-L1398)

## Overview
Reads and parses the final message received from the client in a SCRAM authentication exchange, validating channel binding and extracting the client proof for authentication verification.

## Definition

```c
static void
read_client_final_message(scram_state *state, const char *input)
```
## Detailed Description
This function processes the client-final-message as part of the SCRAM authentication protocol (RFC 5802). It performs several critical validation and parsing tasks:

1. **Channel Binding Validation**: Verifies channel binding data based on whether TLS channel binding is in use
   - For TLS channel binding ('p' flag): Validates against server's SSL certificate hash
   - For no channel binding ('n' or 'y' flags): Validates expected base64-encoded values ("biws" or "eSws")

2. **Nonce Verification**: Extracts and stores the client's final nonce for later verification

3. **Client Proof Extraction**: Decodes the base64-encoded client proof and validates its length

4. **Message Parsing**: Extracts the client-final-message-without-proof portion needed for authentication calculations

The function ensures message integrity and prevents replay attacks through comprehensive validation of all message components.

## Parameters / Member Variables
- : Pointer to scram_state structure that gets populated with:
  - : The nonce from client's final message
  - : Decoded client authentication proof
  - : Message portion used in authentication calculations
- : The complete client-final-message string received from the client

## Dependencies
- Functions called/Symbols referenced:
  - : Parse attribute=value pairs from SCRAM message
  - : Parse any attribute from message (used for extensions)
  - : Get TLS certificate hash for channel binding (SSL builds only)
  - : Calculate base64 encoding length
  - : Encode data to base64 format
  - : Calculate base64 decoding length
  - : Decode base64 data
  - : Allocate memory in current memory context
  - : Duplicate string in current memory context
  - : Free allocated memory
- Called from (representative examples):
  - : Main SCRAM authentication exchange handler

## Notes and Other Information
- The function handles both SSL and non-SSL builds with conditional compilation (#ifdef USE_SSL)
- Channel binding validation differs based on the cbind_flag set during initial handshake
- Supports TLS channel binding using 'tls-server-end-point' method as per RFC 5802
- Includes comprehensive error handling for malformed messages and invalid proofs
- The function modifies the scram_state structure in-place with parsed values
- Supports optional extensions in the message format while ensuring backward compatibility
- All memory allocations use PostgreSQL's memory context system for automatic cleanup