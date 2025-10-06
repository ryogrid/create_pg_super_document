# inet_recv

## Location
[src/backend/utils/adt/network.c:250-257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L250-L257)

## Overview
PostgreSQL type input function that deserializes inet network addresses from external binary format.

## Definition
```c
Datum inet_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `inet_recv` function serves as the binary input function for PostgreSQL's inet data type. It acts as a thin wrapper around the `network_recv` helper function, specifically configured for inet type semantics. The function extracts the binary data buffer from the function arguments and delegates the actual deserialization work to `network_recv` with the `is_cidr` parameter set to false, indicating that CIDR-specific validation rules should not be applied.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro providing access to function arguments and context. The first argument contains the StringInfo buffer with serialized binary data.

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_POINTER`: Extracts the StringInfo buffer pointer from function arguments
  - [network_recv](../n/network_recv.md): Core deserialization function for network addresses
  - `PG_RETURN_INET_P`: Returns the deserialized inet value as a PostgreSQL Datum
- Called from (representative examples):
  - PostgreSQL binary protocol handlers when receiving inet values
  - Binary copy operations involving inet columns
  - Replication and backup/restore operations

## Notes and Other Information
- Part of PostgreSQL's type system infrastructure for binary I/O operations
- The `is_cidr` parameter is set to false, allowing host bits to be set in the address
- Binary format is more efficient than text format for network communication and storage
- Works in conjunction with `inet_send` for complete binary serialization support
- Located in src/backend/utils/adt/network.c:250-257

## Simplified Source

```c
Datum
inet_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);

    // Delegate to network_recv with is_cidr=false
    PG_RETURN_INET_P(network_recv(buf, false));
}
```