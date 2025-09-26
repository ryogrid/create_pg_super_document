# macaddr8

## Location
[src/include/utils/inet.h:107-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/inet.h#L107-L117)

## Overview
The  struct represents the internal storage format for 8-byte MAC addresses in PostgreSQL, providing support for 64-bit Extended Unique Identifiers (EUI-64) and modern networking protocols that use extended MAC address formats.

## Definition

```c
typedef struct macaddr8
{
	unsigned char a;
	unsigned char b;
	unsigned char c;
	unsigned char d;
	unsigned char e;
	unsigned char f;
	unsigned char g;
	unsigned char h;
} macaddr8;
```
## Detailed Description
The  structure is PostgreSQL's internal representation for 8-byte MAC addresses, defined in . This structure extends the traditional 6-byte MAC address format to accommodate 64-bit Extended Unique Identifiers (EUI-64) used in modern networking protocols like IPv6 link-local addresses and some industrial networking standards.

The structure stores an 8-byte MAC address as eight individual unsigned char fields (a through h), corresponding to the eight octets of an extended MAC address. Like its 6-byte counterpart , this structure uses separate byte fields for direct octet access and optimal memory alignment. The  type supports all standard MAC address operations and provides interoperability with the traditional  format through conversion functions.

## Parameters / Member Variables
- `a`: First octet of the 8-byte MAC address (most significant byte)
- `b`: Second octet of the 8-byte MAC address
- `c`: Third octet of the 8-byte MAC address
- `d`: Fourth octet of the 8-byte MAC address
- `e`: Fifth octet of the 8-byte MAC address
- `f`: Sixth octet of the 8-byte MAC address
- `g`: Seventh octet of the 8-byte MAC address
- `h`: Eighth octet of the 8-byte MAC address (least significant byte)
## Dependencies
- Functions called/Symbols referenced: None (primitive struct definition)
- Called from (representative examples):
  -  - Input function for 8-byte MAC address parsing
  -  - Output function for 8-byte MAC address formatting
  -  - Comparison function for 8-byte MAC addresses
  -  - Equality comparison function
  -  - [Hash](../H/Hash.md) function for 8-byte MAC addresses
  -  - Bitwise AND operation on 8-byte MAC addresses
  -  - Bitwise OR operation on 8-byte MAC addresses
  -  - Bitwise NOT operation on 8-byte MAC addresses
  -  - 8-byte MAC address truncation function
  -  - Function to set the 7th bit for EUI-64 conversion
  -  - Conversion function to standard macaddr format
  -  - Conversion function from standard macaddr format

## Notes and Other Information
- This structure supports 8-byte (64-bit) MAC addresses used in EUI-64 format and modern networking protocols
- EUI-64 identifiers are commonly derived from 48-bit MAC addresses by inserting 0xFFFF or 0xFFFE in the middle and flipping the universal/local bit
- The structure provides full interoperability with the traditional 6-byte  format through conversion functions
- PostgreSQL's  function specifically supports the EUI-64 bit manipulation requirements
- The 8-byte format is increasingly important for IPv6 addressing and modern industrial networking protocols
- Memory layout consists of eight consecutive bytes, maintaining efficiency for storage and network operations
- The structure serves as the underlying storage for PostgreSQL's  SQL data type
- Supports all standard bitwise operations (AND, OR, NOT) and comparison operations similar to the 6-byte format