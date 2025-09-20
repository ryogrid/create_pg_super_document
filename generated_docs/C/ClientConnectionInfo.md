# ClientConnectionInfo

## Location
[src/include/libpq/libpq-be.h:90-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/libpq-be.h#L90-L110)

## Overview
ClientConnectionInfo is a structure that contains fields describing client connection information that needs to be copied over to parallel workers in PostgreSQL.

## Definition

```c
typedef struct ClientConnectionInfo
{
	/*
	 * Authenticated identity.  The meaning of this identifier is dependent on
	 * auth_method; it is the identity (if any) that the user presented during
	 * the authentication cycle, before they were assigned a database role.
	 * (It is effectively the "SYSTEM-USERNAME" of a pg_ident usermap --
	 * though the exact string in use may be different, depending on pg_hba
	 * options.)
	 *
	 * authn_id is NULL if the user has not actually been authenticated, for
	 * example if the "trust" auth method is in use.
	 */
	const char *authn_id;

	/*
	 * The HBA method that determined the above authn_id.  This only has
	 * meaning if authn_id is not NULL; otherwise it's undefined.
	 */
	UserAuth	auth_method;
} ClientConnectionInfo;
```
## Detailed Description
ClientConnectionInfo is designed to hold essential client authentication information that must be preserved when creating parallel workers. Unlike the Port structure, ClientConnectionInfo only contains the minimal subset of connection data needed by parallel processes. All memory allocations within this structure must be done using malloc() or palloc() in TopMemoryContext to ensure proper memory management across process boundaries.

The structure serves as a lightweight container for authentication state that can be serialized and transferred to parallel worker processes, ensuring they have access to the client's authenticated identity and the method used to establish that identity.

## Parameters / Member Variables
- `*authn_id`: The authenticated identity of the user as presented during the authentication cycle, before database role assignment. This represents the "SYSTEM-USERNAME" equivalent in pg_ident usermap terms. NULL if no actual authentication occurred (e.g., when using "trust" method).
- `auth_method`: The HBA (Host-Based Authentication) method that determined the authn_id. Only meaningful when authn_id is not NULL.
## Dependencies
- Functions called/Symbols referenced:
  - UserAuth (enum/type for authentication methods)
- Called from (representative examples):
  - [GetUserNameFromId](../G/GetUserNameFromId.md) (in src/backend/utils/init/miscinit.c:1064)
  - Referenced in libpq-be.h header definitions

## Notes and Other Information
- When adding new members to this structure, serialization must be handled in SerializeClientConnectionInfo() and related functions
- All allocations must use TopMemoryContext for proper memory management
- This structure is specifically designed for parallel worker scenarios where only essential authentication information needs to be preserved
- The authn_id field may differ from the actual database username, as it represents the pre-role-assignment identity