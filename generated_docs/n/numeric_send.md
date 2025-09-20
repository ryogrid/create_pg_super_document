# numeric_send

## Location
[src/backend/utils/adt/numeric.c:1161-1193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L1161-L1193)

## Overview
This function serializes a PostgreSQL Numeric value into its external binary representation for network transmission or storage, converting the internal format to a standardized binary protocol format.

## Definition

```c
Datum
numeric_send(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the binary output function for PostgreSQL's Numeric data type. It takes an internal Numeric value and converts it to the external binary representation used in PostgreSQL's binary protocol. The function extracts the numeric components (ndigits, weight, sign, dscale, and digit array) from the internal representation and serializes them as a sequence of int16 values using PostgreSQL's message protocol functions. This binary format is the counterpart to what  expects to deserialize, ensuring round-trip compatibility for network communication and binary storage.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro which provides:
  - : The Numeric value to be serialized to binary format

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC
  - [init_var_from_num](../i/init_var_from_num.md)
  - [pq_begintypsend](../p/pq_begintypsend.md)
  - [pq_sendint16](../p/pq_sendint16.md)
  - [pq_endtypsend](../p/pq_endtypsend.md)
  - PG_RETURN_BYTEA_P
- Called from:
  - Used as a PostgreSQL type output function (registered in system catalogs)

## Notes and Other Information
- This is a PostgreSQL function interface (uses PG_FUNCTION_ARGS/PG_RETURN_BYTEA_P macros)
- Returns a bytea (byte array) containing the serialized binary representation
- The binary format consists of: ndigits, weight, sign, dscale, followed by all digit values
- Each component is serialized as a 16-bit integer using network byte order
- Counterpart function to numeric_recv for complete binary serialization/deserialization
- Essential for PostgreSQL's binary protocol communication and efficient storage
- Located in src/backend/utils/adt/numeric.c:1161-1193
- Uses PostgreSQL's standard type send/receive protocol infrastructure