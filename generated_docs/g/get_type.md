# get_type

## Location
[src/interfaces/ecpg/preproc/type.c:133-240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.c#L133-L240)

## Overview
A static utility function that converts ECPG type enumeration values to their corresponding string representations for code generation purposes.

## Definition


## Detailed Description
The  function serves as a type-to-string converter within the ECPG preprocessor's type system. It takes an enumerated type value from the ECPGttype enumeration and returns the corresponding string literal that represents that type in generated code. This function is essential for code generation, allowing the preprocessor to output the appropriate type identifiers when generating C code for embedded SQL operations. The function covers all standard C data types supported by ECPG, as well as special PostgreSQL-specific types like varchar, bytea, decimal, numeric, interval, and various date/time types.

## Parameters / Member Variables
- : An enumeration value of type ECPGttype representing the data type to be converted to string

## Dependencies
- Functions called/Symbols referenced:
  - mmerror (error reporting function)
  - PARSE_ERROR (error type constant)
  - [ET_ERROR](../E/ET_ERROR.md) (error level constant)
  - ECPGttype (enumeration type)
  - All ECPGt_* enumeration values (various type constants)
- Called from (representative examples):
  - [ECPGdump_a_simple](../E/ECPGdump_a_simple.md) (for generating simple type representations)

## Notes and Other Information
- This is a static function, only accessible within the same source file
- Returns NULL after calling mmerror for unrecognized type codes
- Covers all fundamental C types (char, int, float, double, etc.) and their unsigned variants
- Includes PostgreSQL-specific types like decimal, numeric, interval, timestamp, bytea
- Handles special ECPG types like ECPGt_NO_INDICATOR, ECPGt_char_variable, ECPGt_const
- The function uses a comprehensive switch statement with explicit break statements for clarity
- Located in  at lines 133-240