# PgBackendGSSStatus

## Location
[src/include/utils/backend_status.h:74-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/backend_status.h#L74-L82)

## Overview
PgBackendGSSStatus is a structure that stores GSSAPI (Generic Security Services Application Program Interface) connection information for each PostgreSQL backend process in shared memory, providing GSS authentication and encryption status details.

## Definition
```c
typedef struct PgBackendGSSStatus
{
    /* Information about GSSAPI connection */
    char        gss_princ[NAMEDATALEN]; /* GSSAPI Principal used to auth */
    bool        gss_auth;               /* If GSSAPI authentication was used */
    bool        gss_enc;                /* If encryption is being used */
    bool        gss_delegation;         /* If credentials delegated */

} PgBackendGSSStatus;
```

## Detailed Description
PgBackendGSSStatus is a shared-memory data structure that maintains GSSAPI-specific information for each backend connection. This structure is only populated when GSS (GSSAPI/Kerberos) is enabled for a connection. It serves as part of PostgreSQL's backend status tracking system, allowing administrators and monitoring tools to inspect GSS connection details across all active database sessions.

Like PgBackendSSLStatus, this structure works in conjunction with the main PgBackendStatus structure and is allocated separately in shared memory to optimize space usage - it's only created when GSSAPI connections are present.

The structure tracks both authentication and encryption aspects of GSSAPI connections, as well as credential delegation status, providing comprehensive GSS session information for security monitoring and administration.

## Parameters / Member Variables
- `gss_princ[NAMEDATALEN]`: The GSSAPI principal name used for authentication (e.g., "user@REALM.COM")
- `gss_auth`: Boolean flag indicating whether GSSAPI authentication was used for this connection
- `gss_enc`: Boolean flag indicating whether GSSAPI encryption is being used for this connection
- `gss_delegation`: Boolean flag indicating whether credentials have been delegated for this connection

## Dependencies
- Constants referenced:
  - NAMEDATALEN (used for sizing the principal name field)
- Used by:
  - NumBackendStatSlots (for memory calculation)
  - [BackendStatusShmemSize](../B/BackendStatusShmemSize.md) (for shared memory sizing)
  - [CreateSharedBackendStatus](../C/CreateSharedBackendStatus.md) (for initialization)
  - [pgstat_bestart](../p/pgstat_bestart.md) (for populating GSS status)
  - pgstat_read_current_status (for reading GSS information)
  - [PgBackendStatus](PgBackendStatus.md) (as a member structure)

## Notes and Other Information
- This structure is only allocated and populated when GSSAPI is enabled for a connection
- The gss_princ field must be null-terminated like all character arrays in backend status structures
- Supports both authentication-only and encryption-enabled GSSAPI configurations
- The gss_delegation flag is important for security auditing as it indicates whether the client has delegated credentials to the server
- Part of PostgreSQL's backend status monitoring infrastructure accessible through system views like pg_stat_gssapi
- Stored in shared memory to allow cross-process access for monitoring and administrative queries
- Works alongside Kerberos and other GSSAPI-compatible authentication mechanisms