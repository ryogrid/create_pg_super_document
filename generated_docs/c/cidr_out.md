# cidr_out

## Location
src/backend/utils/adt/network.c: 173 - 191

## Overview
Converts a CIDR (Classless Inter-Domain Routing) network address from internal PostgreSQL format to its external string representation.

## Definition


## Detailed Description
The  function is a PostgreSQL type output function that converts a CIDR network address from its internal binary representation to a human-readable string format. It serves as the external representation function for the CIDR data type, which is used to represent network addresses with subnet masks in PostgreSQL. The function delegates the actual formatting work to the  helper function, specifying that it should format the output as a CIDR value (with the subnet mask included).

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments and context. The first argument (index 0) contains the inet/cidr value to be converted.

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts the inet pointer from function arguments
  - : Common formatting function for inet/cidr values  
  - : Returns a null-terminated C string result
- Called from (representative examples):
  - PostgreSQL type system when converting CIDR values to text
  - SQL queries that need to display CIDR values as strings

## Notes and Other Information
- This function is part of PostgreSQL's type system infrastructure for the CIDR data type
- The actual formatting logic is handled by  with the  parameter set to 
- CIDR format includes the subnet mask (e.g., "192.168.1.0/24") unlike inet format
- Located in src/backend/utils/adt/network.c:173-191