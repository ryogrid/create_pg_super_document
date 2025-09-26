# hashinet

## Location
[src/backend/utils/adt/network.c:880-889](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L880-L889)

## Overview
Provides hash function support for inet/cidr data types to enable hash indexing on network address columns.

## Definition

```c
structure */
	return hash_any((unsigned char *) VARDATA_ANY(addr), addrsize + 2);
```
## Detailed Description
This function serves as a support function for hash indexes on inet and cidr data types in PostgreSQL. It computes a hash value for network addresses by extracting the binary representation of the inet/cidr value and applying PostgreSQL's general-purpose hash_any function. The function assumes there are no padding bytes in the inet data structure when calculating the hash.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP - retrieves inet argument from function call
  - ip_addrsize - determines the size of the IP address portion
  - [hash_any](hash_any.md) - PostgreSQL's generic hash function
  - VARDATA_ANY - macro to get variable-length data portion
- Called from (representative examples):
  - No direct references found in the codebase (likely called via function pointer in hash index operations)

## Notes and Other Information
- The function includes a comment noting the assumption about no padding bytes in the inet data structure
- The hash calculation includes the address size plus 2 additional bytes, likely for the inet structure metadata
- This function is typically registered as part of the hash operator class for inet/cidr types rather than called directly
- [Hash](../H/Hash.md) functions like this are essential for the performance of hash joins and hash-based aggregations on network address data