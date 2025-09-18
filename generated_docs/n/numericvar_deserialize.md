# numericvar_deserialize

## Location
src/backend/utils/adt/numeric.c: 7756 - 7778

## Overview
Deserializes binary data back into a NumericVar structure, reconstructing the internal representation from the format created by numericvar_serialize.

## Definition
```c
static void numericvar_deserialize(StringInfo buf, NumericVar *var)
```

## Detailed Description
This function reconstructs a NumericVar structure from binary data stored in a StringInfo buffer. It is the counterpart to numericvar_serialize, reading the same binary format that was written during serialization. The function first reads the number of digits to determine how much memory to allocate, then reads the metadata fields (weight, sign, dscale) and finally reads each individual digit value.

The function uses alloc_var to properly allocate memory for the digit array based on the deserialized length, ensuring the NumericVar structure is properly initialized before populating it with data.

## Parameters / Member Variables
- `buf`: StringInfo buffer containing the serialized binary data to be read
- `var`: Pointer to the NumericVar structure that will be populated with deserialized data

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint](../p/pq_getmsgint.md) (for reading ndigits, weight, sign, dscale fields and digit values)
  - [alloc_var](../a/alloc_var.md) (for allocating memory for the NumericVar structure)
- Called from (representative examples):
  - [numeric_avg_deserialize](numeric_avg_deserialize.md)
  - [numeric_deserialize](numeric_deserialize.md)
  - [numeric_poly_deserialize](numeric_poly_deserialize.md)
  - [int8_avg_deserialize](../i/int8_avg_deserialize.md)

## Notes and Other Information
- This is a static function, only accessible within the numeric.c file
- The function reads 32-bit integers for metadata fields (ndigits, weight, sign, dscale) and 16-bit integers for digit values
- Memory allocation is handled automatically through alloc_var, which sets var->ndigits appropriately
- The deserialization format matches the serialization format used by numericvar_serialize
- Primarily used for restoring aggregate function states in parallel query processing
- No validation is performed on the deserialized values, maintaining consistency with the serialization approach