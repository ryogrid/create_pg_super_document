# decoct

## Location
[src/port/inet_net_ntop.c:155-177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/inet_net_ntop.c#L155-L177)

## Overview
Converts a sequence of bytes to dot-separated decimal notation, used as a utility function for formatting IPv6 address components.

## Definition

```c
struct
	{
		int			base,
					len;
	}			best, cur;
```
## Detailed Description
This static utility function converts a series of bytes from binary format to dot-separated decimal notation. Each byte is formatted as a decimal number (0-255) and separated by dots, except for the last byte which has no trailing dot. The function is designed to handle the conversion of byte sequences within IPv6 address formatting, where certain parts of the address may need to be represented in decimal notation rather than hexadecimal.

The function ensures proper null-termination of the output string and tracks the buffer size to prevent overflow.

## Parameters / Member Variables
- : Pointer to the byte array to convert
- : Number of bytes to process from the source
- : Output buffer to store the formatted string
- : Size of the destination buffer

## Dependencies
- Functions called/Symbols referenced:
  - SPRINTF (macro for formatted string output)
- Called from (representative examples):
  - [inet_net_ntop_ipv6](../i/inet_net_ntop_ipv6.md)

## Notes and Other Information
- Returns the number of characters written to dst, or 0 on buffer overflow
- Each byte is converted to its decimal representation (0-255)
- Bytes are separated by dots, with no trailing dot after the last byte
- Ensures null-termination of the output string
- Performs buffer size checking to prevent overflow
- Used specifically in IPv6 address formatting contexts