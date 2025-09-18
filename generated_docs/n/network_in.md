# network_in

## Location
src/backend/utils/adt/network.c: 75 - 120

## Overview
A common input parsing function for INET and CIDR data types that converts string representations of network addresses into PostgreSQL's internal inet structure.

## Definition


## Detailed Description
This function serves as the core parsing routine for both INET and CIDR data types in PostgreSQL. It takes a string representation of an IP address (with optional network mask) and converts it into PostgreSQL's internal inet structure. The function automatically detects whether the input is an IPv4 or IPv6 address by checking for the presence of colons, then uses the appropriate parsing logic. For CIDR inputs, it performs additional validation to ensure no bits are set beyond the network mask.

## Parameters / Member Variables
- : String representation of the network address to parse
- : Boolean flag indicating whether to enforce CIDR validation rules
- : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - inet (data type)
  - palloc0 (memory allocation)
  - strchr (string search)
  - pg_inet_net_pton (network address parsing)
  - ip_family, ip_addr, ip_bits, ip_addrsize, ip_maxbits (inet accessor macros)
  - addressOK (CIDR validation)
  - SET_INET_VARSIZE (size setting macro)
  - ereturn (error return macro)
  - PGSQL_AF_INET, PGSQL_AF_INET6 (address family constants)
- Called from (representative examples):
  - inet_in (INET type input function)
  - cidr_in (CIDR type input function)
  - inet_client_addr (client address retrieval)
  - inet_server_addr (server address retrieval)

## Notes and Other Information
- The function is marked as static, indicating it's an internal helper function within the network.c module
- IPv6 detection is performed by checking for the presence of ':' characters in the input string
- For CIDR types, additional validation ensures the address portion doesn't have bits set beyond the network mask
- Uses PostgreSQL's error context system for proper error handling and reporting
- The function allocates memory using palloc0 to ensure the inet structure is zero-initialized