# pq_getmsgfloat8

## Location
[src/backend/libpq/pqformat.c:488-507](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqformat.c#L488-L507)

## Overview
Extracts a 64-bit double-precision floating-point number from a message buffer using IEEE 754 binary format conversion.

## Definition

```c
union
	{
		float8		f;
		int64		i;
	}			swap;
```
## Detailed Description
The `pq_getmsgfloat8` function reads an 8-byte double-precision floating-point value from a message buffer by utilizing the 64-bit integer parsing infrastructure. Similar to pq_getmsgfloat4, it uses a union to safely convert between the binary representation (int64) and the floating-point representation (float8) without violating strict aliasing rules. The function leverages pq_getmsgint64 to read the 8 bytes and handle network byte order conversion, then interprets those bytes as an IEEE 754 double-precision floating-point number. This design ensures cross-platform compatibility for double-precision floating-point data transmission in PostgreSQL's binary protocol.

## Parameters / Member Variables
- `msg`: A StringInfo structure containing the message buffer data, length, and current cursor position

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint64](pq_getmsgint64.md) (reads 8 bytes as int64 with byte order conversion)
  - float8 (PostgreSQL's double-precision float type)
- Called from (representative examples):
  - [float8recv](../f/float8recv.md) (float8 data type receive function)
  - [box_recv](../b/box_recv.md) (geometric box type receive)
  - [point_recv](point_recv.md) (geometric point type receive)
  - [line_recv](../l/line_recv.md) (geometric line type receive)
  - [circle_recv](../c/circle_recv.md) (geometric circle type receive)
  - [complex_recv](../c/complex_recv.md) (tutorial complex type receive)

## Notes and Other Information
- Uses a union for safe type punning between int64 and float8
- Relies on pq_getmsgint64 for network byte order conversion and 64-bit integer handling
- Follows IEEE 754 double-precision floating-point format
- Extensively used in geometric data types that require double-precision coordinates
- Part of PostgreSQL's binary protocol for double-precision float data types
- The function is defined in src/backend/libpq/pqformat.c:488-507
- Designed to work with the corresponding pq_sendfloat8 function
- Ensures cross-platform compatibility for double-precision floating-point data transmission
- More widely used than pq_getmsgfloat4 due to prevalence of double-precision arithmetic in geometric and mathematical operations