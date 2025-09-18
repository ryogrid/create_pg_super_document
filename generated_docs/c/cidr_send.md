# cidr_send

## Location
[src/backend/utils/adt/network.c:300-308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L300-L308)

## Overview
Converts a CIDR data type to its binary representation for transmission over the network or storage in binary format.

## Definition
```c
Datum cidr_send(PG_FUNCTION_ARGS)
```

## Detailed Description
The `cidr_send` function is a PostgreSQL I/O function that serializes a CIDR data type into its binary wire format. This function is part of the PostgreSQL type system that enables efficient binary transmission of network addresses between client and server or for binary storage. It serves as the output function for binary format operations on CIDR types.

The function is a thin wrapper around the internal `network_send` function, specifically configured for CIDR types (as opposed to INET types). It extracts the INET value from the function arguments and delegates the actual serialization work to `network_send` with the `is_cidr` parameter set to true, indicating that this represents a CIDR block rather than a host address.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: INET pointer (`inet *`) - the network address to be converted to binary format (note: CIDR internally uses the same inet structure)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INET_PP: Macro to extract INET argument from function args
  - [network_send](../n/network_send.md): Internal function that performs the actual binary serialization
  - PG_RETURN_BYTEA_P: Macro to return bytea result
- Called from (representative examples):
  - No direct callers found in codebase (likely called by PostgreSQL type system infrastructure)

## Notes and Other Information
- This function is registered as the binary output function for the CIDR data type in PostgreSQL's type system
- Although CIDR and INET types use the same internal structure (`inet`), the `is_cidr` flag in the binary format distinguishes between them
- The binary format includes family, netmask bits, CIDR flag (set to true), address length, and the raw address bytes
- Used internally by PostgreSQL for binary protocol communication and binary storage formats
- Located in src/backend/utils/adt/network.c:300-308