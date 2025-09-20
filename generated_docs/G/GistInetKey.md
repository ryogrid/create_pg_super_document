# GistInetKey

## Location
[src/backend/utils/adt/network_gist.c:79-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_gist.c#L79-L86)

## Overview
A specialized data structure used to represent INET/CIDR index keys in PostgreSQL's GiST (Generalized Search Tree) indexing system for network address types.

## Definition

```c
typedef struct GistInetKey
{
	uint8		va_header;		/* varlena header --- don't touch directly */
	unsigned char family;		/* PGSQL_AF_INET, PGSQL_AF_INET6, or zero */
	unsigned char minbits;		/* minimum number of bits in netmask */
	unsigned char commonbits;	/* number of common prefix bits in addresses */
	unsigned char ipaddr[16];	/* up to 128 bits of common address */
} GistInetKey;
```
## Detailed Description
GistInetKey is a specialized representation used internally by PostgreSQL's GiST indexing mechanism for INET and CIDR network address data types. Unlike the standard INET/CIDR representation, this structure includes additional metadata necessary for efficient GiST tree operations, specifically tracking the length of common address prefixes and minimum netmask lengths.

The structure follows PostgreSQL's varlena header rules to maintain compatibility with the core GiST infrastructure while providing the specialized functionality needed for network address indexing. It uses a 1-byte-header varlena format for simplicity and efficiency.

## Parameters / Member Variables
- : Standard PostgreSQL varlena header for variable-length data types (should not be accessed directly)
- : Address family identifier (PGSQL_AF_INET for IPv4, PGSQL_AF_INET6 for IPv6, or zero for unspecified)
- : Minimum number of bits required in the netmask for addresses represented by this key
- : Number of bits that are common across all addresses represented by this key (used for prefix compression)
- : Binary representation of the common address prefix (up to 128 bits for IPv6, with IPv4 using only the first 4 bytes)

## Dependencies
- Functions called/Symbols referenced:
  - Standard PostgreSQL varlena header operations
  - PGSQL_AF_INET and PGSQL_AF_INET6 constants

- Called from (representative examples):
  - DatumGetInetKeyP (macro for datum conversion)
  - SET_GK_VARSIZE (macro for setting variable size)
  - [inet_gist_consistent](../i/inet_gist_consistent.md) (GiST consistency function)
  - [calc_inet_union_params](../c/calc_inet_union_params.md) (union parameter calculation)
  - [build_inet_union_key](../b/build_inet_union_key.md) (union key construction)
  - [inet_gist_union](../i/inet_gist_union.md) (GiST union operation)
  - [inet_gist_compress](../i/inet_gist_compress.md) (GiST compression function)
  - [inet_gist_fetch](../i/inet_gist_fetch.md) (GiST fetch operation)
  - [inet_gist_penalty](../i/inet_gist_penalty.md) (GiST penalty calculation)
  - [inet_gist_picksplit](../i/inet_gist_picksplit.md) (GiST split operation)
  - [inet_gist_same](../i/inet_gist_same.md) (GiST equality test)

## Notes and Other Information
- The structure is designed to be compatible with PostgreSQL's varlena system while providing specialized functionality for network address indexing
- IPv4 addresses use only the first 4 bytes of the ipaddr array, while IPv6 addresses can use all 16 bytes
- The commonbits field enables efficient prefix-based operations essential for network address range queries
- Access macros are provided for consistent field access: gk_ip_family(), gk_ip_minbits(), gk_ip_commonbits(), gk_ip_addr()
- Size calculation macros account for the variable address length based on the address family
- Located in src/backend/utils/adt/network_gist.c:79-86