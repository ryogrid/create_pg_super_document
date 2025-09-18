# macaddr_in

## Location
[src/backend/utils/adt/mac.c:55-120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac.c#L55-L120)

## Overview
This function parses a MAC address from a string representation and converts it into PostgreSQL's internal macaddr data type, supporting multiple common MAC address notations.

## Definition


## Detailed Description
The  function is the input function for PostgreSQL's macaddr data type. It accepts a string representation of a MAC address and converts it to the internal binary format. The function supports multiple common MAC address notations:

1. Colon-separated format: 
2. Hyphen-separated format: 
3. Grouped colon format: 
4. Grouped hyphen format: 
5. Dot-separated format: 
6. Mixed format: 
7. Continuous format: 

The function validates that each octet is within the valid range (0-255) and returns appropriate error messages for invalid input. It uses multiple  calls to try different formats until one succeeds.

## Parameters / Member Variables
- : Input string containing the MAC address representation (retrieved via )
- : Error context for soft error handling
- : Pointer to the resulting macaddr structure
- : Integer variables to hold the six octets of the MAC address
- : Buffer to detect trailing garbage characters
- : Number of successfully parsed components

## Dependencies
- Functions called/Symbols referenced:
  - : Standard C library function for formatted input parsing
  - : PostgreSQL error return macro
  - : PostgreSQL memory allocation function
  - : PostgreSQL macro to return macaddr pointer
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL type conversion)

## Notes and Other Information
- The function tries multiple format patterns in sequence until one matches exactly 6 components
- Validates that all octets are in the range 0-255
- Uses  in sscanf patterns to detect trailing garbage characters
- Returns appropriate PostgreSQL error codes for invalid input syntax or out-of-range values
- Memory for the result is allocated using PostgreSQL's memory management system