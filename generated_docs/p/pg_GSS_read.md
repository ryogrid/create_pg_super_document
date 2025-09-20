# pg_GSS_read

## Location
[src/interfaces/libpq/fe-secure-gssapi.c:266-434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-gssapi.c#L266-L434)

## Overview
Reads encrypted data from a GSSAPI-secured PostgreSQL connection, handling packet reception, decryption, and data buffering for the caller.

## Definition

```c
ssize_t
pg_GSS_read(PGconn *conn, void *ptr, size_t len)
```
## Detailed Description
This function receives and decrypts data from a GSSAPI-encrypted connection. It manages the complex process of reading encrypted packets that include length headers, validating packet sizes, decrypting the payload using GSS-API, and buffering the decrypted data for gradual consumption by the caller. The function handles partial packet reception and maintains internal buffers to ensure complete packets are processed atomically.

Key behaviors:
- Reads packets with 4-byte network-order length headers followed by encrypted payload
- Validates packet sizes against PQ_GSS_MAX_PACKET_SIZE limits
- Decrypts complete packets using gss_unwrap
- Maintains result buffer (PqGSSResultBuffer) for decrypted data
- Handles partial reads from both network and result buffer
- Ensures encrypted packets use confidentiality protection

## Parameters / Member Variables
- : PostgreSQL connection object containing GSSAPI context and internal buffers
- : Output buffer where decrypted data will be copied
- : Maximum number of bytes to read into the output buffer

## Dependencies
- Functions called/Symbols referenced:
  - gss_unwrap (GSSAPI function for decryption)
  - gss_release_buffer (GSSAPI buffer cleanup)
  - [pqsecure_raw_read](pqsecure_raw_read.md) (low-level socket read function)
  - [pg_GSS_error](pg_GSS_error.md) (error reporting function)
  - pg_ntoh32 (network to host byte order conversion)
  - [libpq_gettext](../l/libpq_gettext.md) (internationalization)
- Called from:
  - [pqsecure_read](pqsecure_read.md) (main secure read dispatch function)

## Notes and Other Information
- Uses global state variables PqGSSRecvBuffer, PqGSSRecvLength for incoming encrypted packets
- Uses PqGSSResultBuffer, PqGSSResultLength, PqGSSResultNext for managing decrypted data
- Implements buffering strategy where partial result data is preserved across function calls
- Returns immediately when any decrypted data is available rather than waiting for complete buffer fill
- Validates that all incoming packets use confidentiality protection
- Sets errno to EWOULDBLOCK for incomplete packet reception scenarios