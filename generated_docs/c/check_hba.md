# check_hba

## Location
[src/backend/libpq/hba.c:2469-2582](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L2469-L2582)

## Overview
Scans the pre-parsed HBA (Host-Based Authentication) configuration to find a matching rule for an incoming connection request and assigns the appropriate authentication method.

## Definition

```c
struct sockaddr *) &hba->addr,
									  (struct sockaddr *) &hba->mask))
							continue;
```
## Detailed Description
This function implements the core logic of PostgreSQL's host-based authentication system. It iterates through the parsed pg_hba.conf rules (stored in parsed_hba_lines) and applies a series of filters to find the first matching rule for the incoming connection. The matching process follows a strict hierarchical evaluation:

1. **Connection Type Matching**: Distinguishes between local (Unix socket) and network connections, and for network connections, checks SSL and GSSAPI encryption states
2. **Address Matching**: For network connections, validates the client's IP address against the rule's address specification (individual IP, CIDR block, hostname, or special keywords like 'all', 'samehost', 'samenet')  
3. **Database Matching**: Ensures the requested database is allowed by the rule
4. **Role Matching**: Verifies the connecting user/role is permitted by the rule

If a matching rule is found, it assigns that HbaLine to the port. If no rules match, it creates an implicit rejection rule with uaImplicitReject authentication method, ensuring that unmatched connections are always denied.

## Parameters / Member Variables
- : Pointer to hbaPort structure containing connection details (user, database, client address, SSL state, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [get_role_oid](../g/get_role_oid.md) (role name to OID resolution)
  - [check_hostname](check_hostname.md) (hostname-based address matching)
  - [check_ip](check_ip.md) (IP address and netmask matching)
  - [check_same_host_or_net](check_same_host_or_net.md) (samehost/samenet address matching)
  - [check_db](check_db.md) (database name matching)
  - [check_role](check_role.md) (role/user name matching)
  - [palloc0](../p/palloc0.md) (memory allocation)
  - Connection type constants (ctLocal, ctHostSSL, ctHostNoSSL, ctHostGSS, ctHostNoGSS)
  - IP comparison method constants (ipCmpMask, ipCmpAll, ipCmpSameHost, ipCmpSameNet)
  - Authentication method constants (uaImplicitReject)
- Called from:
  - [hba_getauthmethod](../h/hba_getauthmethod.md) (src/backend/libpq/hba.c:3050)

## Notes and Other Information
- Uses the global parsed_hba_lines list which contains pre-parsed HBA configuration rules
- Follows PostgreSQL's "first match wins" policy - stops at the first matching rule
- GSSAPI encryption checking is conditionally compiled based on ENABLE_GSS
- The function always assigns an HBA rule to the port, either a matching rule or an implicit rejection
- Does not perform the actual authentication - only determines which authentication method should be used
- Critical security function as it controls access to the database based on connection origin and credentials