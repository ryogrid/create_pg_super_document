# set_int_item

## Location
[src/interfaces/ecpg/ecpglib/descriptor.c:151-193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/descriptor.c#L151-L193)

## Overview
Converts and extracts an integer value from a variable of various numeric data types with appropriate type casting for ECPG descriptor operations.

## Definition


## Detailed Description
This internal utility function is the counterpart to get_int_item, serving as part of ECPG's dynamic descriptor implementation. It extracts a numeric value from a source variable of various types and converts it to an integer for storage in a target integer pointer. The function handles the reverse operation of get_int_item by reading from typed variables and producing integer values.

The function supports the same comprehensive range of numeric types as get_int_item, performing appropriate type conversion from the source variable type to integer. It ensures type safety by validating that the source variable type is numeric before attempting the conversion.

## Parameters / Member Variables  
- : Source code line number for error reporting and debugging
- : Pointer to integer where the converted value will be stored
- : Generic const pointer to the source variable containing the value to convert
- : ECPG type enumeration specifying the source variable's data type

## Dependencies
- Functions called/Symbols referenced:
  - ECPGttype
  - ECPGt_short
  - ECPGt_int (implicitly through ECPGttype)
  - ECPGt_long
  - ECPGt_unsigned_short
  - ECPGt_unsigned_int
  - ECPGt_unsigned_long
  - ECPGt_long_long
  - ECPGt_unsigned_long_long
  - ECPGt_float
  - ECPGt_double
  - [ecpg_raise](../e/ecpg_raise.md)
  - ECPG_VAR_NOT_NUMERIC
  - ECPG_SQLSTATE_RESTRICTED_DATA_TYPE_ATTRIBUTE_VIOLATION
- Called from (representative examples):
  - [ECPGset_desc](../E/ECPGset_desc.md) (multiple calls for different descriptor field assignments)

## Notes and Other Information
- Static function, only accessible within descriptor.c
- Complementary function to get_int_item, handling the reverse conversion operation
- Handles both signed and unsigned integer types, as well as floating-point types
- Returns true on successful conversion, false on type validation error
- Provides automatic type conversion from various numeric types to integer
- Essential component of ECPG's descriptor field value extraction mechanism
- Validates source type is numeric before attempting conversion