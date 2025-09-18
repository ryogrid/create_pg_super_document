# macaddr_and

## Location
src/backend/utils/adt/mac.c: 303 - 319

## Overview
Performs bitwise AND operation between two MAC addresses, returning a new MAC address with each byte being the result of the AND operation on corresponding bytes.

## Definition


## Detailed Description
The  function implements bitwise AND operation for PostgreSQL's  data type. It takes two MAC addresses as input arguments and computes the bitwise AND of each corresponding byte pair (a through f) to produce a new MAC address. This function is typically used in network address masking operations where you need to apply a bitmask to a MAC address. The function allocates memory for the result using PostgreSQL's memory management system () and returns the result using PostgreSQL's function call convention.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument:  - The first MAC address operand
  - Second argument:  - The second MAC address operand (used as mask)

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts macaddr arguments from function call
  -  - PostgreSQL memory allocation function
  -  - Returns macaddr result following PostgreSQL conventions
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Each byte of the MAC address (a, b, c, d, e, f) is processed independently using bitwise AND
- Memory for the result is allocated in the current memory context
- This function follows PostgreSQL's V1 calling convention for built-in functions
- Commonly used for network address masking operations in MAC address processing