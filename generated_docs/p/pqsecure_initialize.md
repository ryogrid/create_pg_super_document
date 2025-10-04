# pqsecure_initialize

## Location
[src/interfaces/libpq/fe-secure.c:138-152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure.c#L138-L152)

## Overview
Initializes the global SSL context for a PostgreSQL connection with configurable SSL and crypto library initialization options.

## Definition
```c
int pqsecure_initialize(PGconn *conn, bool do_ssl, bool do_crypto)
```

## Detailed Description
pqsecure_initialize is an internal function that serves as a wrapper for SSL context initialization within libpq. It provides the actual implementation for SSL initialization by calling the underlying pgtls_init function when SSL support is compiled in. This function is typically called during connection establishment to ensure that the SSL infrastructure is properly set up before attempting secure communication.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn connection structure that will use the SSL context
- `do_ssl`: Boolean flag indicating whether to initialize SSL-related components
- `do_crypto`: Boolean flag indicating whether to initialize crypto-related components

## Dependencies
- Functions called/Symbols referenced:
  - [pgtls_init](pgtls_init.md)
  - USE_SSL (conditional compilation flag)
  - PostgresPollingStatusType
- Called from (representative examples):
  - CONNECTION_FAILED context in src/interfaces/libpq/fe-connect.c:3351
  - CONNECTION_FAILED context in src/interfaces/libpq/fe-connect.c:3535
  - Referenced in src/interfaces/libpq/libpq-int.h:765

## Notes and Other Information
- Returns 0 on success when SSL support is not compiled in (USE_SSL undefined)
- Returns the result of pgtls_init when SSL support is available
- This is an internal libpq function, not part of the public API
- Used primarily during connection establishment phase
- The function handles both SSL and crypto initialization in a single call
- Failure in this function typically results in CONNECTION_FAILED status

## Simplified Source

```c
int pqsecure_initialize(PGconn *conn, bool do_ssl, bool do_crypto) {
    int r = 0;

    // Initialize SSL/TLS if support is compiled in
    #ifdef USE_SSL
        r = pgtls_init(conn, do_ssl, do_crypto);
    #endif

    return r;
}
```