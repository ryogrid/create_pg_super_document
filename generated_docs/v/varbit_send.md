# varbit_send

## Location
[src/backend/utils/adt/varbit.c:681-701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L681-L701)

## Overview
Converts PostgreSQL's internal VarBit representation to external binary format for data transmission over the binary protocol.

## Definition
```c
Datum varbit_send(PG_FUNCTION_ARGS)
```

## Detailed Description
The `varbit_send` function is responsible for serializing PostgreSQL's internal VarBit data structure into the binary wire format used in PostgreSQL's binary protocol. This is the counterpart to `varbit_recv` and is used when sending bit string data to clients that request binary format results. The function creates a binary representation consisting of a 32-bit integer indicating the bit length, followed by the raw byte data containing the actual bits.

The function uses PostgreSQL's standard binary serialization API, beginning with `pq_begintypsend` to initialize the output buffer, then writing the bit length and byte data, and finally completing the serialization with `pq_endtypsend`. The resulting bytea is returned to the caller for transmission.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `s`: Input VarBit pointer obtained via `PG_GETARG_VARBIT_P(0)`

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARBIT_P
  - [pq_begintypsend](../p/pq_begintypsend.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - pq_sendbytes
  - [pq_endtypsend](../p/pq_endtypsend.md)
  - VARBITLEN
  - VARBITS
  - VARBITBYTES
  - PG_RETURN_BYTEA_P
- Called from:
  - [bit_send](../b/bit_send.md)

## Notes and Other Information
- Counterpart function to `varbit_recv` for binary protocol I/O
- Uses PostgreSQL's standard binary serialization framework (pq_*typsend functions)
- The binary format is: [int32 bit_length][byte_array bit_data]
- Automatically handles the correct number of bytes needed via VARBITBYTES macro
- Returns a bytea object containing the serialized binary data
- Part of PostgreSQL's type system for efficient binary data transmission
- Located in src/backend/utils/adt/varbit.c:681-701