# PgBackendSSLStatus

## Location
[src/include/utils/backend_status.h:49-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/backend_status.h#L49-L64)

## Overview
PgBackendSSLStatus is a structure that stores SSL connection information for each PostgreSQL backend process in shared memory, providing detailed SSL session data for monitoring and administrative purposes.

## Definition

```c
typedef struct PgBackendSSLStatus
{
	/* Information about SSL connection */
	int			ssl_bits;
	char		ssl_version[NAMEDATALEN];
	char		ssl_cipher[NAMEDATALEN];
	char		ssl_client_dn[NAMEDATALEN];

	/*
	 * serial number is max "20 octets" per RFC 5280, so this size should be
	 * fine
	 */
	char		ssl_client_serial[NAMEDATALEN];

	char		ssl_issuer_dn[NAMEDATALEN];
} PgBackendSSLStatus;
```
## Detailed Description
PgBackendSSLStatus is a shared-memory data structure that maintains SSL-specific information for each backend connection. This structure is only populated when SSL is enabled for a connection. It serves as part of PostgreSQL's backend status tracking system, allowing administrators and monitoring tools to inspect SSL connection details across all active database sessions.

The structure is designed to work in conjunction with the main PgBackendStatus structure and is allocated separately in shared memory to optimize space usage - it's only created when SSL connections are present.

All character arrays in this structure must be null-terminated, ensuring safe string operations when accessing the SSL information.

## Parameters / Member Variables
- : The key length in bits used for the SSL connection encryption
- : The SSL/TLS protocol version being used (e.g., "TLSv1.2", "TLSv1.3")  
- : The name of the cipher suite being used for encryption
- : The Distinguished Name (DN) from the client's SSL certificate
- : The serial number from the client's SSL certificate (max 20 octets per RFC 5280)
- : The Distinguished Name (DN) of the Certificate Authority that issued the client certificate

## Dependencies
- Constants referenced:
  - NAMEDATALEN (used for sizing all string fields)
- Used by:
  - NumBackendStatSlots (for memory calculation)
  - [BackendStatusShmemSize](../B/BackendStatusShmemSize.md) (for shared memory sizing)
  - [CreateSharedBackendStatus](../C/CreateSharedBackendStatus.md) (for initialization)
  - [pgstat_bestart](../p/pgstat_bestart.md) (for populating SSL status)
  - pgstat_read_current_status (for reading SSL information)
  - [PgBackendStatus](PgBackendStatus.md) (as a member structure)

## Notes and Other Information
- This structure is only allocated and populated when SSL is enabled for a connection
- All string fields are sized using NAMEDATALEN to ensure consistent memory layout
- The ssl_client_serial field size follows RFC 5280 specifications for certificate serial numbers
- Part of PostgreSQL's backend status monitoring infrastructure accessible through system views like pg_stat_ssl
- Stored in shared memory to allow cross-process access for monitoring and administrative queries