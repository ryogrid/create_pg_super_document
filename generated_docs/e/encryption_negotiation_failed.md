# encryption_negotiation_failed

## Location
src/interfaces/libpq/fe-connect.c: 4356 - 4380

## Overview
`encryption_negotiation_failed` is a static function that handles the failure of an encryption method during connection negotiation, marking the failed method and attempting to select the next available encryption method.

## Definition
```c
static int encryption_negotiation_failed(PGconn *conn)
```

## Detailed Description
This function is called when an encryption method (SSL, GSSAPI, or plaintext) fails during connection establishment. It marks the current encryption method as failed by setting the corresponding bit in the `failed_enc_methods` bitmask, then attempts to select the next available encryption method using `select_next_encryption_method()`. The function returns different values based on the outcome: 0 if no more encryption methods are available (connection should fail), 1 if another method is available and can be tried with the existing connection, or 2 if a new connection is required (specifically for direct SSL negotiation mode). The special case for direct SSL occurs when SSL is the selected method and direct SSL negotiation is configured, requiring a fresh connection attempt.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn structure containing connection state and encryption method tracking

## Dependencies
- Functions called/Symbols referenced:
  - `select_next_encryption_method`: Selects the next encryption method to attempt
  - `Assert`: Debug assertion to verify failed method is not current method
  - Encryption method constants: `ENC_SSL`

- Called from (representative examples):
  - Connection state machine in `ENCRYPTION_NEGOTIATION_FAILED` state (line 2876)
  - Internal connection option processing (line 397)

## Notes and Other Information
This function is part of the encryption negotiation state machine in PostgreSQL libpq. It implements a fallback mechanism where if one encryption method fails (e.g., SSL handshake failure), the client can automatically try other configured methods (e.g., GSSAPI or plaintext) without requiring the application to handle the retry logic. The return value of 2 for direct SSL indicates that the connection socket must be closed and a new one established, as direct SSL negotiation cannot fallback on the same socket connection. The assertion ensures that the failed method is properly tracked and not mistakenly marked as both failed and current simultaneously.