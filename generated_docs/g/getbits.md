# getbits

## Location
[src/backend/utils/adt/inet_net_pton.c:349-381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/inet_net_pton.c#L349-L381)

## Overview
Parses and validates a numeric string representing bit count for network prefix lengths, ensuring proper format and range validation.

## Definition


## Detailed Description
This utility function parses a string containing a decimal number and validates it as a bit count for network prefix specifications. It performs strict validation to ensure the input contains only digits, has no leading zeros (except for the single digit '0'), and falls within the valid range for network prefix lengths.

The function is primarily used to parse CIDR prefix lengths in network address specifications. It enforces a maximum value of 128 bits to accommodate both IPv4 (32 bits max) and IPv6 (128 bits max) prefix lengths. The function returns success or failure status and stores the parsed value in the provided output parameter.

The validation includes checking for leading zeros to prevent octal interpretation and ensures that only valid decimal digits are present in the input string.

## Parameters / Member Variables
- : Source string containing the decimal number to parse
- : Pointer to integer where the parsed bit count will be stored

## Dependencies
- Functions called/Symbols referenced:
  - Standard C library function: strchr
- Called from (representative examples):
  - [getv4](getv4.md) (src/backend/utils/adt/inet_net_pton.c:413)
  - [inet_cidr_pton_ipv6](../i/inet_cidr_pton_ipv6.md) (src/backend/utils/adt/inet_net_pton.c:513)

## Notes and Other Information
- Returns 1 on successful parsing, 0 on failure
- Enforces maximum value of 128 bits to support both IPv4 and IPv6 prefix lengths
- Prohibits leading zeros to prevent ambiguous number interpretation
- Only accepts pure decimal digit strings - any non-digit character causes failure
- Used primarily for parsing CIDR prefix lengths in network address parsing functions
- The function is strict about format - empty strings or strings with no digits return failure