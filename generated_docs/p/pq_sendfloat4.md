# pq_sendfloat4

## Location
src/backend/libpq/pqformat.c: 252 - 275

## Overview
Appends a float4 (single-precision floating-point) value to a StringInfo buffer in PostgreSQL's external binary representation format.

## Definition


## Detailed Description
The pq_sendfloat4 function handles the serialization of single-precision floating-point values (float4) into PostgreSQL's standardized binary wire format. This function encapsulates the knowledge of how float4 values should be represented externally, ensuring portability across different architectures and systems.

The implementation uses a union to perform a type-punning operation, treating the float4 value as a 32-bit unsigned integer without changing its bit representation. This allows the function to leverage the existing pq_sendint32() function for proper byte-order handling and network transmission. The assumption is that float4 values should be byte-swapped in the same manner as 32-bit integers, which provides reasonable portability across most IEEE-float-using architectures.

## Parameters / Member Variables
- : StringInfo buffer to append the serialized float4 value to
- : float4 value to be serialized and appended

## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendint32](pq_sendint32.md) (handles the actual serialization and byte-swapping of the 32-bit representation)
  - float4 (PostgreSQL's single-precision floating-point type)
  - uint32 (32-bit unsigned integer type used in the union)

- Called from (representative examples):
  - [float4send](../f/float4send.md) (float4 datatype's send function for binary output)

## Notes and Other Information
- Uses a union for type-punning to convert float4 to uint32 without changing bit representation
- Relies on pq_sendint32 for proper byte-order handling and network serialization
- The byte-swapping assumption works well across most IEEE-float-using architectures but is not universally perfect
- Part of PostgreSQL's type-specific serialization system for binary protocol communication
- Localizes knowledge of external binary representation, making the codebase more maintainable
- Essential for binary format output of floating-point columns in query results