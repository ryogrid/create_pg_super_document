# numericvar_serialize

## Location
src/backend/utils/adt/numeric.c: 7740 - 7755

## Overview
Serializes a NumericVar structure to binary format for storage or transmission, allowing intermediate values with higher precision than the standard numeric type.

## Definition


## Detailed Description
This function serializes a NumericVar structure into a binary format using PostgreSQL's string buffer mechanism. The function performs no validation on weight or dscale values, enabling the serialization of intermediate computational results that may exceed the precision limits of the standard numeric data type. This design choice supports internal calculations that require higher precision before final rounding.

The serialization format differs from the wire protocol used by numeric_send/recv functions, as it uses 32-bit integers for weight and dscale fields instead of 16-bit integers, allowing for a broader range of intermediate values.

## Parameters / Member Variables
- : StringInfo buffer where the serialized data will be written
- : Pointer to the NumericVar structure to be serialized (const, indicating read-only access)

## Dependencies
- Functions called/Symbols referenced:
  - [pq_sendint32](../p/pq_sendint32.md) (for ndigits, weight, sign, dscale fields)
  - [pq_sendint16](../p/pq_sendint16.md) (for individual digit values)
- Called from (representative examples):
  - [numeric_avg_serialize](numeric_avg_serialize.md)
  - [numeric_serialize](numeric_serialize.md)  
  - [numeric_poly_serialize](numeric_poly_serialize.md)
  - [int8_avg_serialize](../i/int8_avg_serialize.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the numeric.c file
- The function uses 32-bit integers for metadata fields (ndigits, weight, sign, dscale) but 16-bit integers for digit values
- No validation is performed on the input NumericVar, allowing for intermediate values that exceed normal numeric type constraints
- The serialization format is incompatible with the standard numeric_send/recv wire protocol due to different field sizes
- Primarily used for aggregate function state serialization in parallel query processing