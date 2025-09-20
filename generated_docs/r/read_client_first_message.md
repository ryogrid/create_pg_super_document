# read_client_first_message

## Location
[src/backend/libpq/auth-scram.c:899-1112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L899-L1112)

## Overview
A comprehensive parser for the initial client message in the SCRAM-SHA-256 authentication protocol, validating the message format and extracting essential authentication parameters.

## Definition

```c
static void
read_client_first_message(scram_state *state, const char *input)
```
## Detailed Description
The `read_client_first_message` function implements the complete parsing logic for the client-first-message as defined in RFC 5802 (SCRAM protocol specification). It performs rigorous validation of the message structure, including the GS2 header format, channel binding flags, and authentication attributes.

The function processes the complex SCRAM message format which includes:
- GS2 header with channel binding flags (n/y/p)
- Optional authorization identity handling (which PostgreSQL rejects)
- Username extraction (though PostgreSQL uses the startup packet username instead)  
- Client nonce validation for printable characters
- Channel binding type validation (supporting only "tls-server-end-point")
- Extension handling (rejecting unsupported mandatory extensions)

The parser maintains strict protocol compliance and provides detailed error messages for any malformed input, making it essential for secure SCRAM authentication in PostgreSQL.

## Parameters / Member Variables
- `state`: Pointer to the scram_state structure that stores authentication session data, including extracted client information and protocol flags
- `input`: The raw client-first-message string received from the client during authentication

## Dependencies
- Functions called/Symbols referenced:
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication)
  - [sanitize_char](../s/sanitize_char.md) (at Line 990, 1020, 1052, 1067)
  - [read_attr_value](read_attr_value.md) (at Line 1035, 1089, 1092)  
  - [sanitize_str](../s/sanitize_str.md) (at Line 1045)
  - [is_scram_printable](../i/is_scram_printable.md) (at Line 1093)
  - [read_any_attr](read_any_attr.md) (at Line 1103)
  - ereport/errcode/errmsg/errdetail (PostgreSQL error reporting system)
- Called from (representative examples):
  - [scram_exchange](../s/scram_exchange.md) (at src/backend/libpq/auth-scram.c:395)
  - scram_state (at src/backend/libpq/auth-scram.c:173)

## Notes and Other Information
- Implements RFC 5802 SCRAM protocol specification for client-first-message parsing
- Supports both SCRAM-SHA-256 and SCRAM-SHA-256-PLUS variants with channel binding
- Channel binding validation ensures protocol security requirements are met
- Username from SCRAM message is ignored in favor of PostgreSQL's startup packet username
- Comprehensive error handling with specific error codes for different protocol violations
- Sets up critical state information including client_nonce, client_username, cbind_flag, and client_first_message_bare
- Handles optional extensions by skipping unknown attributes while rejecting mandatory ones