# inet_in

## Location
src/backend/utils/adt/network.c: 121 - 128

## Overview
The input function for the INET data type that converts string representations of IP addresses into PostgreSQL's internal inet format.

## Definition


## Detailed Description
This function serves as the standard input conversion function for PostgreSQL's INET data type. It acts as a thin wrapper around the network_in function, specifically configured for INET semantics (as opposed to CIDR). The function extracts the input string from PostgreSQL's function call interface and delegates the actual parsing work to network_in with the is_cidr parameter set to false, allowing host bits to be set beyond the network mask.

## Parameters / Member Variables
- Uses PostgreSQL's standard function interface (PG_FUNCTION_ARGS)
  - Argument 0: C-string representation of the inet address

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (extract string argument)
  - network_in (common parsing function)
  - PG_RETURN_INET_P (return inet value)
- Called from (representative examples):
  - PG_STAT_GET_ACTIVITY_COLS (statistics collection)
  - pg_stat_get_backend_client_addr (backend client address retrieval)

## Notes and Other Information
- This is a PostgreSQL built-in function that can be called from SQL
- Passes false for the is_cidr parameter to network_in, allowing more flexible address formats than CIDR
- Uses PostgreSQL's standard function calling convention with Datum return type
- Part of the INET/CIDR family of network data types in PostgreSQL
- Handles both IPv4 and IPv6 address formats through the underlying network_in function