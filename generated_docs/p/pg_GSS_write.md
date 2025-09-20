# pg_GSS_write

## Location
[src/interfaces/libpq/fe-secure-gssapi.c:93-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-gssapi.c#L93-L265)

## Overview
Writes data to a GSSAPI-encrypted PostgreSQL connection, handling encryption, packetization, and partial transmission retry logic.

## Definition

```c
ssize_t
pg_GSS_write(PGconn *conn, const void *ptr, size_t len)
```
## Detailed Description
This function encrypts and transmits data over a GSSAPI-secured connection. It handles the complete lifecycle of encrypting plaintext data using GSS-API, packaging it into properly formatted packets with length headers, and managing partial writes and retries. The function implements robust error handling and maintains state for partial transmission scenarios where the underlying socket would block.

Key behaviors:
- Encrypts data in chunks up to PqGSSMaxPktSize bytes
- Packages encrypted data with 4-byte network-order length headers
- Maintains transmission state across calls for retry scenarios
- Ensures all-or-nothing semantics for the caller (reports success only when all data is processed)
- Validates confidentiality of encrypted packets

## Parameters / Member Variables
- : PostgreSQL connection object containing GSSAPI context and buffers
- : Pointer to the data buffer to be encrypted and transmitted
- : Number of bytes to encrypt and send

## Dependencies
- Functions called/Symbols referenced:
  - gss_wrap (GSSAPI function for encryption)
  - gss_release_buffer (GSSAPI buffer cleanup)
  - [pqsecure_raw_write](pqsecure_raw_write.md) (low-level socket write function)
  - [pg_GSS_error](pg_GSS_error.md) (error reporting function)
  - pg_hton32 (network byte order conversion)
  - [libpq_gettext](../l/libpq_gettext.md) (internationalization)
- Called from:
  - [pqsecure_write](pqsecure_write.md) (main secure write dispatch function)

## Notes and Other Information
- Uses global state variables PqGSSSendBuffer, PqGSSSendLength, PqGSSSendNext, and PqGSSSendConsumed for managing partial transmissions
- Maximum packet size is limited by PQ_GSS_MAX_PACKET_SIZE constant
- Requires that the GSSAPI transport negotiation be complete before use
- Implements careful retry logic where callers must re-send the exact same data on retryable failures
- Returns the number of input bytes consumed on success, -1 on error with errno set appropriately