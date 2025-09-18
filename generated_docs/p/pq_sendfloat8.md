# pq_sendfloat8

## Location
[src/backend/libpq/pqformat.c:276-295](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqformat.c#L276-L295)

## Overview
Appends a float8 (double-precision floating-point) value to a StringInfo buffer in PostgreSQL's external binary representation format.

## Definition


## Detailed Description
The pq_sendfloat8 function handles the serialization of double-precision floating-point values (float8) into PostgreSQL's standardized binary wire format. This function serves as the counterpart to pq_sendfloat4, but operates on 64-bit double-precision values instead of 32-bit single-precision values.

Similar to pq_sendfloat4, this function uses a union to perform type-punning, treating the float8 value as a 64-bit signed integer without altering its bit representation. This approach allows the function to delegate the actual byte-order handling and network serialization to pq_sendint64(). The implementation assumes that float8 values should be byte-swapped in the same manner as 64-bit integers, providing reasonable portability across most IEEE-float-using architectures.

## Parameters / Member Variables
- : StringInfo buffer to append the serialized float8 value to
- : float8 value to be serialized and appended

## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendint64](pq_sendint64.md) (handles the actual serialization and byte-swapping of the 64-bit representation)
  - float8 (PostgreSQL's double-precision floating-point type)
  - int64 (64-bit signed integer type used in the union)

- Called from (representative examples):
  - [float8send](../f/float8send.md) (float8 datatype's send function for binary output)
  - [box_send](../b/box_send.md), line_send, point_send, lseg_send, poly_send, circle_send (geometric types that contain float8 coordinates)
  - [complex_send](../c/complex_send.md) (tutorial example of complex number type)

## Notes and Other Information
- Uses a union for type-punning to convert float8 to int64 without changing bit representation
- Relies on pq_sendint64 for proper byte-order handling and network serialization
- The byte-swapping assumption provides good portability across IEEE-float-using architectures
- Extensively used by PostgreSQL's geometric types (box, line, point, etc.) which store coordinates as float8 values
- Part of PostgreSQL's comprehensive type serialization system for binary protocol communication
- Localizes knowledge of external binary representation, improving code maintainability
- Critical for binary format output of double-precision numeric data and geometric types