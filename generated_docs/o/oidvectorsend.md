# oidvectorsend

## Location
[src/backend/utils/adt/oid.c:226-234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oid.c#L226-L234)

## Overview
Binary send function that converts PostgreSQL's internal oidvector format to external binary format for network transmission.

## Definition

```c
Datum
oidvectorsend(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a type send function that handles the conversion from PostgreSQL's internal oidvector data structure to the external binary protocol format. This function is the counterpart to  and is part of the binary I/O infrastructure used for network communication and binary data exchange.

The implementation is remarkably simple - it directly delegates all work to the general  function, which handles the binary serialization of array types. Since oidvector is implemented as a specialized array type with OID elements, the generic array sending logic is sufficient for proper binary encoding.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention macro that provides:
## Dependencies
- Functions called/Symbols referenced:
  - [array_send](../a/array_send.md) (general array send function)
- Called from (representative examples):
  - PostgreSQL binary protocol handling
  - Network communication during data transfer
  - Binary format data export operations

## Notes and Other Information
- Extremely simple implementation that delegates entirely to array_send
- Takes advantage of oidvector's implementation as a specialized array type
- Part of PostgreSQL's binary I/O protocol for efficient data transfer
- The counterpart to oidvectorrecv for complete binary I/O support
- No special handling needed beyond generic array serialization
- Efficient reuse of existing array infrastructure