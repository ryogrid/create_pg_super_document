# outDouble

## Location
[src/backend/nodes/outfuncs.c:211-226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L211-L226)

## Overview
Converts a double-precision floating-point value to its shortest decimal representation for PostgreSQL node serialization, ensuring exact value preservation.

## Definition
static void outDouble(StringInfo str, double d)

## Detailed Description
The outDouble function is responsible for serializing double-precision floating-point numbers in PostgreSQL's node output system. Its primary goal is to preserve the exact value of the double when it is later parsed back from the serialized representation.

The function uses PostgreSQL's double_to_shortest_decimal_buf utility function, which implements an algorithm to convert the double to its shortest possible decimal representation that still preserves the original value exactly. This is important because not all floating-point values can be represented exactly in decimal form, but the algorithm ensures that when the decimal string is parsed back to a double, the result will be bit-for-bit identical to the original value.

The function allocates a buffer on the stack with size DOUBLE_SHORTEST_DECIMAL_LEN, which is specifically sized to accommodate the longest possible shortest decimal representation of any double value. This approach is both efficient and safe, avoiding dynamic memory allocation while ensuring sufficient space for any double value.

## Parameters / Member Variables
- `str`: StringInfo buffer where the decimal representation will be appended
- `d`: Double-precision floating-point value to be converted

## Dependencies
- Functions called/Symbols referenced:
  - DOUBLE_SHORTEST_DECIMAL_LEN (constant defining buffer size for shortest decimal representation)
  - [double_to_shortest_decimal_buf](../d/double_to_shortest_decimal_buf.md) (utility function for exact double-to-decimal conversion)
  - [appendStringInfoString](../a/appendStringInfoString.md) (for appending the resulting decimal string)

- Called from (representative examples):
  - WRITE_FLOAT_FIELD (macro in outfuncs.c:80)

## Notes and Other Information
- This function is declared static, limiting its scope to the outfuncs.c file
- The 'shortest decimal' approach is crucial for maintaining floating-point precision across serialization/deserialization cycles
- Uses stack allocation for efficiency, avoiding heap overhead for the temporary conversion buffer
- Part of PostgreSQL's broader strategy for preserving data fidelity in node serialization
- The exact preservation property is essential for query plan consistency and correctness
- Handles special values like NaN, infinity, and negative zero appropriately through the underlying conversion function