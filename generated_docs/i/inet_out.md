# inet_out

## Location
src/backend/utils/adt/network.c: 165 - 172

## Overview
The output function for the INET data type that converts PostgreSQL's internal inet format into string representations.

## Definition


## Detailed Description
This function serves as the standard output conversion function for PostgreSQL's INET data type. It acts as a thin wrapper around the network_out function, specifically configured for INET formatting semantics. The function extracts the inet value from PostgreSQL's function call interface and delegates the actual formatting work to network_out with the is_cidr parameter set to false, which means it won't force the inclusion of mask notation unless it's naturally part of the address representation.

## Parameters / Member Variables
- Uses PostgreSQL's standard function interface (PG_FUNCTION_ARGS)
  - Argument 0: inet value to be formatted as a string

## Dependencies
- Functions called/Symbols referenced:
  - inet (data type)
  - PG_GETARG_INET_PP (extract inet argument)
  - [network_out](../n/network_out.md) (common formatting function)
  - PG_RETURN_CSTRING (return C-string value)
- Called from (representative examples):
  - No direct references found in the current analysis

## Notes and Other Information
- This is a PostgreSQL built-in function that can be called from SQL during output operations
- Passes false for the is_cidr parameter to network_out, allowing flexible output formatting
- Uses PostgreSQL's standard function calling convention with Datum return type
- Part of the INET/CIDR family of network data types in PostgreSQL
- Handles both IPv4 and IPv6 address formats through the underlying network_out function
- The output format may or may not include mask notation depending on how the inet value was originally created
- More flexible than cidr_out as it doesn't enforce mandatory mask notation in the output