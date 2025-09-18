# cidr_in

## Location
src/backend/utils/adt/network.c: 129 - 140

## Overview
The input function for the CIDR data type that converts string representations of network addresses into PostgreSQL's internal inet format with strict CIDR validation.

## Definition


## Detailed Description
This function serves as the standard input conversion function for PostgreSQL's CIDR data type. It acts as a thin wrapper around the network_in function, specifically configured for CIDR semantics which enforce strict network address validation. Unlike inet_in, this function passes true for the is_cidr parameter to network_in, ensuring that no host bits are set beyond the network mask, maintaining proper CIDR block representation.

## Parameters / Member Variables
- Uses PostgreSQL's standard function interface (PG_FUNCTION_ARGS)
  - Argument 0: C-string representation of the CIDR address

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (extract string argument)
  - network_in (common parsing function)
  - PG_RETURN_INET_P (return inet value)
- Called from (representative examples):
  - No direct references found in the current analysis

## Notes and Other Information
- This is a PostgreSQL built-in function that can be called from SQL
- Passes true for the is_cidr parameter to network_in, enforcing strict CIDR validation rules
- Uses PostgreSQL's standard function calling convention with Datum return type
- Part of the INET/CIDR family of network data types in PostgreSQL
- Handles both IPv4 and IPv6 CIDR blocks through the underlying network_in function
- CIDR validation ensures that all host bits beyond the network mask are zero, representing proper network blocks
- More restrictive than inet_in as it requires proper network block format