# check_network_data

## Location
[src/backend/libpq/hba.c:55-60](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L55-L60)

## Overview
A structure that serves as callback data for network interface enumeration during host-based authentication (HBA) processing in PostgreSQL.

## Definition

```c
typedef struct check_network_data
{
	IPCompareMethod method;		/* test method */
	SockAddr   *raddr;			/* client's actual address */
	bool		result;			/* set to true if match */
} check_network_data;
```
## Detailed Description
The `check_network_data` structure is used as a parameter passing mechanism for the `check_network_callback` function during network interface enumeration. It encapsulates all the necessary information needed to determine whether a client's network address matches any of the server's network interfaces, which is essential for implementing PostgreSQL's "samehost" and "samenet" authentication rules defined in pg_hba.conf.

This structure is designed to work with the `pg_foreach_ifaddr` function, which iterates through all available network interfaces on the system. The callback function uses the data in this structure to perform IP address comparisons according to the specified method.

## Parameters / Member Variables
- `method`: Specifies the IP comparison method to use (from IPCompareMethod enum: ipCmpMask, ipCmpSameHost, ipCmpSameNet, ipCmpAll)
- `raddr`: Pointer to the client's actual socket address that needs to be matched against server interfaces
- `result`: Boolean flag that gets set to true when a match is found; initialized to false and updated by the callback function

## Dependencies
- Functions called/Symbols referenced:
  - [IPCompareMethod](../I/IPCompareMethod.md) (enum type)
  - [SockAddr](../S/SockAddr.md) (type alias for socket address structures)
- Called from (representative examples):
  - [check_network_callback](check_network_callback.md) (uses this structure as callback data)
  - [check_same_host_or_net](check_same_host_or_net.md) (initializes and uses this structure)

## Notes and Other Information
- This structure is specifically designed for use with PostgreSQL's host-based authentication system
- The structure is passed by reference to callback functions during network interface enumeration
- The `result` field acts as an output parameter that gets modified by the callback function
- Used internally by the HBA (Host-Based Authentication) subsystem to implement "samehost" and "samenet" connection rules
- The structure is defined in src/backend/libpq/hba.c and is not exposed in public headers, indicating it's an internal implementation detail