# connection_failed

## Location
[src/interfaces/libpq/fe-connect.c:4381-4395](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L4381-L4395)

## Overview
`connection_failed` is a static function that serves as the out-of-line implementation of the CONNECTION_FAILED() macro, handling connection failures by marking the current encryption method as failed and selecting the next method to retry.

## Definition
```c
static bool connection_failed(PGconn *conn)
```

## Detailed Description
This function implements the core logic for handling connection failures in the encryption negotiation process. When a connection attempt fails, it marks the current encryption method as failed by setting the corresponding bit in the `failed_enc_methods` bitmask, then calls `select_next_encryption_method()` to determine if another encryption method should be attempted. The function is designed as the out-of-line portion of the CONNECTION_FAILED() macro to handle the retry logic centrally. Unlike `encryption_negotiation_failed()`, this function is called for general connection failures (not specifically encryption negotiation failures) and always passes `false` to `select_next_encryption_method()`, indicating this is not a negotiation-specific failure.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn structure containing connection state and encryption method tracking

## Dependencies
- Functions called/Symbols referenced:
  - [select_next_encryption_method](../s/select_next_encryption_method.md): Selects the next encryption method to attempt (called with `false` parameter)
  - `Assert`: Debug assertion to verify the failed method is not currently active

- Called from (representative examples):
  - CONNECTION_FAILED macro usage (line 2897)
  - Internal connection option processing (line 399)

## Notes and Other Information
This function is part of the encryption method fallback mechanism in PostgreSQL libpq. It provides a simplified interface compared to `encryption_negotiation_failed()` by always returning a boolean indicating whether to retry with a different encryption method, rather than the more complex return codes. The function is implemented as a separate function rather than inline in the macro to reduce code duplication throughout the connection state machine. The assertion ensures proper state management by verifying that a method is not simultaneously marked as both current and failed. This function enables automatic retry with different encryption methods when connection attempts fail, improving connection reliability in environments with mixed encryption support.