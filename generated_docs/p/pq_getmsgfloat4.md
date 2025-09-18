# pq_getmsgfloat4

## Location
src/backend/libpq/pqformat.c: 469 - 487

## Overview
Extracts a 32-bit floating-point number from a message buffer using IEEE 754 binary format conversion.

## Definition


## Detailed Description
The `pq_getmsgfloat4` function reads a 4-byte floating-point value from a message buffer by leveraging the existing integer parsing infrastructure. It uses a union to safely convert between the binary representation (uint32) and the floating-point representation (float4) without violating strict aliasing rules. The function first reads the 4 bytes as an integer using pq_getmsgint, which handles network byte order conversion, then interprets those bytes as an IEEE 754 single-precision floating-point number. This approach ensures portability across different architectures while maintaining the correct binary representation.

## Parameters / Member Variables
- `msg`: A StringInfo structure containing the message buffer data, length, and current cursor position

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint](pq_getmsgint.md) (reads 4 bytes as integer with byte order conversion)
  - float4 (PostgreSQL's single-precision float type)
- Called from (representative examples):
  - [float4recv](../f/float4recv.md) (float4 data type receive function)

## Notes and Other Information
- Uses a union for safe type punning between uint32 and float4
- Relies on pq_getmsgint for network byte order conversion
- Follows IEEE 754 single-precision floating-point format
- Part of PostgreSQL's binary protocol for float data types
- The function is defined in src/backend/libpq/pqformat.c:469-487
- Designed to work with the corresponding pq_sendfloat4 function
- Ensures cross-platform compatibility for floating-point data transmission
- Limited usage compared to integer message functions, primarily for float4 data type operations