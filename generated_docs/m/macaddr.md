# macaddr

## Location
[src/include/utils/inet.h:94-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/inet.h#L94-L102)

## Overview
The  struct represents the internal storage format for MAC (Media Access Control) addresses in PostgreSQL, providing a 6-byte structure to store standard 48-bit MAC addresses used in network interfaces.

## Definition

```c
typedef struct macaddr
{
	unsigned char a;
	unsigned char b;
	unsigned char c;
	unsigned char d;
	unsigned char e;
	unsigned char f;
} macaddr;
```
## Detailed Description
The  structure is PostgreSQL's internal representation for MAC addresses, defined in . This structure stores a standard 6-byte MAC address as six individual unsigned char fields (a through f), corresponding to the six octets of a MAC address in the format . The structure provides the foundation for PostgreSQL's  data type, enabling storage, comparison, and manipulation of MAC addresses within the database system.

The design uses separate byte fields rather than an array, which allows for direct access to individual octets and potentially better memory alignment on some architectures. This structure is used extensively throughout PostgreSQL's MAC address handling functions for input/output operations, comparisons, and various network-related computations.

## Parameters / Member Variables
- `a`: First octet of the MAC address (most significant byte)
- `b`: Second octet of the MAC address
- `c`: Third octet of the MAC address
- `d`: Fourth octet of the MAC address
- `e`: Fifth octet of the MAC address
- `f`: Sixth octet of the MAC address (least significant byte)
## Dependencies
- Functions called/Symbols referenced: None (primitive struct definition)
- Called from (representative examples):
  -  - Input function for MAC address parsing
  -  - Output function for MAC address formatting
  -  - Comparison function for MAC addresses
  -  - Equality comparison function
  -  - [Hash](../H/Hash.md) function for MAC addresses
  -  - Bitwise AND operation on MAC addresses
  -  - Bitwise OR operation on MAC addresses
  -  - Bitwise NOT operation on MAC addresses
  -  - MAC address truncation function
  -  - Conversion function to macaddr8 format
  -  - Network address to scalar conversion

## Notes and Other Information
- This structure represents the standard 6-byte (48-bit) MAC address format commonly used in Ethernet networking
- PostgreSQL also provides a  structure for 8-byte (64-bit) MAC addresses used in some modern networking contexts
- The structure supports all standard MAC address operations including comparison, hashing, and bitwise operations
- MAC addresses stored in this format can be converted to and from the newer  format using dedicated conversion functions
- The structure is used as the underlying storage for PostgreSQL's  SQL data type
- Memory layout is straightforward with six consecutive bytes, making it efficient for storage and network operations