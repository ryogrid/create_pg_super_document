# cidr_recv

## Location
[src/backend/utils/adt/network.c:258-269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L258-L269)

## Overview
PostgreSQL type input function that deserializes CIDR network addresses from external binary format with strict network validation.

## Definition
```c
Datum cidr_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `cidr_recv` function serves as the binary input function for PostgreSQL's cidr data type. It acts as a wrapper around the `network_recv` helper function, specifically configured for CIDR type semantics. The key difference from `inet_recv` is that this function sets the `is_cidr` parameter to true when calling `network_recv`, which enforces strict CIDR validation rules requiring that no host bits are set beyond the network mask boundary.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro providing access to function arguments and context. The first argument contains the StringInfo buffer with serialized binary data.

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_POINTER`: Extracts the StringInfo buffer pointer from function arguments
  - [network_recv](../n/network_recv.md): Core deserialization function for network addresses
  - `PG_RETURN_INET_P`: Returns the deserialized inet value as a PostgreSQL Datum
- Called from (representative examples):
  - PostgreSQL binary protocol handlers when receiving cidr values
  - Binary copy operations involving cidr columns
  - Replication and backup/restore operations

## Notes and Other Information
- Part of PostgreSQL's type system infrastructure for binary I/O operations
- The `is_cidr` parameter is set to true, enforcing that host bits must be zero beyond the network mask
- CIDR validation ensures the address represents a proper network range, not a host address
- Binary format provides efficient serialization for network communication and storage
- Works in conjunction with `cidr_send` for complete binary serialization support
- Located in src/backend/utils/adt/network.c:258-269