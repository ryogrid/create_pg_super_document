# init_allowed_encryption_methods

## Location
[src/interfaces/libpq/fe-connect.c:4291-4355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L4291-L4355)

## Overview
`init_allowed_encryption_methods` is a static function that initializes the connection encryption state machine based on sslmode and gssencmode settings, determining which encryption methods are allowed for the connection.

## Definition
```c
static bool init_allowed_encryption_methods(PGconn *conn)
```

## Detailed Description
This function sets up the encryption negotiation state machine by analyzing the connection configuration and socket type. For Unix domain sockets, it disables SSL and GSSAPI encryption since they are not supported over local sockets, defaulting to plaintext communication (unless GSSAPI is explicitly required, which causes an error). For network connections, it examines the sslmode and gssencmode parameters to determine which encryption methods should be attempted. The function sets appropriate flags for SSL, GSSAPI, and plaintext methods based on the configuration, with SSL enabled for non-disabled sslmode values (unless GSSAPI is required), GSSAPI enabled for non-disabled gssencmode values, and plaintext allowed when both SSL and GSSAPI modes permit it (disable, prefer, or allow modes).

## Parameters / Member Variables
- `conn`: Pointer to the PGconn structure containing connection configuration and state

## Dependencies
- Functions called/Symbols referenced:
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md): Appends error message when GSSAPI is required over Unix socket
  - [select_next_encryption_method](../s/select_next_encryption_method.md): Selects the next encryption method to try
  - Encryption method constants: `ENC_SSL`, `ENC_GSSAPI`, `ENC_PLAINTEXT`, `ENC_ERROR`
  - Conditional compilation flags: `USE_SSL`, `ENABLE_GSS`

- Called from (representative examples):
  - Connection establishment functions (referenced at line 2972 in CONNECTION_FAILED context)
  - Internal connection option processing (referenced at line 395)

## Notes and Other Information
This function is part of the PostgreSQL libpq encryption negotiation mechanism introduced to support multiple encryption methods. It handles the initialization phase where the client determines which encryption methods are available and permitted based on configuration. The function includes special handling for Unix domain sockets where encryption is not meaningful. The actual selection and negotiation of encryption methods is delegated to `select_next_encryption_method()`. The function returns false only when GSSAPI encryption is required but the connection is over a Unix socket, which is an invalid configuration.