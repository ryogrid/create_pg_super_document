# select_next_encryption_method

## Location
src/interfaces/libpq/fe-connect.c: 4396 - 4399

## Overview
Selects the next encryption method to attempt during PostgreSQL connection establishment, cycling through available encryption options in a predefined priority order.

## Definition
```c
static bool select_next_encryption_method(PGconn *conn, bool have_valid_connection)
```

## Detailed Description
This function is responsible for determining which encryption method to try next when establishing a connection to a PostgreSQL server. It operates as part of the connection retry mechanism, selecting from available encryption methods (GSSAPI, SSL, or plaintext) while respecting client configuration preferences and avoiding previously failed methods.

The function uses a bitmask approach to track allowed, failed, and currently attempted encryption methods. It implements a specific priority order:
1. GSSAPI encryption (if enabled and credentials are available)
2. SSL or plaintext (order depends on sslmode setting)
3. No encryption if no other options remain

For sslmode="allow", plaintext is tried before SSL. For sslmode="prefer", SSL is tried before plaintext. The function updates the connection's current_enc_method field and returns true if a method is selected, false if no options remain.

## Parameters / Member Variables
- `conn`: PGconn structure representing the database connection context
- `have_valid_connection`: Boolean indicating whether there is already a valid connection established

## Dependencies
- Functions called/Symbols referenced:
  - pg_GSS_have_cred_cache (conditionally, if ENABLE_GSS defined)
  - libpq_append_conn_error
- Called from (representative examples):
  - init_allowed_encryption_methods (fe-connect.c:4338)
  - encryption_negotiation_failed (fe-connect.c:4361) 
  - connection_failed (fe-connect.c:4386)

## Notes and Other Information
- The function uses a local SELECT_NEXT_METHOD macro to streamline the selection logic
- GSSAPI support is conditional on ENABLE_GSS compilation flag
- The function respects the sslmode configuration parameter to determine SSL vs plaintext priority
- Failed encryption methods are tracked in conn->failed_enc_methods to avoid retrying
- Sets conn->current_enc_method to ENC_ERROR when no methods remain
- Located in src/interfaces/libpq/fe-connect.c:4396-4462