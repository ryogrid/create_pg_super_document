# enum_send

## Location
[src/backend/utils/adt/enum.c:221-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/enum.c#L221-L251)

## Overview
Converts internal enum OID values to binary protocol format for efficient data transmission in PostgreSQL's binary I/O operations.

## Definition

```c
Datum
enum_send(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the binary output conversion for PostgreSQL enum types, transforming internal OID values into binary protocol format for transmission. It serves as the counterpart to enum_recv, handling the conversion from internal representation to binary wire format used in operations like COPY BINARY and prepared statements with binary result formats.

The function looks up the enum label corresponding to the given OID and packages it into a binary message buffer using PostgreSQL's binary protocol functions. Unlike enum_out which returns a simple C string, enum_send creates a properly formatted binary message that includes length information and proper encoding for network transmission.

## Parameters / Member Variables
-  (PG_GETARG_OID(0)): The internal OID representation of the enum value to convert to binary format

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_enum (referenced twice for struct access)
  - [pq_begintypsend](../p/pq_begintypsend.md)
  - [pq_sendtext](../p/pq_sendtext.md)
  - [pq_endtypsend](../p/pq_endtypsend.md)
  - PG_RETURN_BYTEA_P
- Called from (representative examples):
  - No direct references found (called via function manager for binary protocol)

## Notes and Other Information
- This is part of PostgreSQL's binary I/O support system, complementing enum_out for text output
- Uses the pq_* family of functions to create properly formatted binary protocol messages
- More efficient than text-based output for bulk data operations and network transmission
- Does not require check_safe_enum_use validation since it's converting from valid internal representation
- Essential for COPY BINARY operations and prepared statements with binary result formats
- The binary format includes proper length encoding and is platform-independent
- Creates a bytea result that contains the binary-encoded enum label
- Part of the complete binary I/O support for enum types alongside enum_recv