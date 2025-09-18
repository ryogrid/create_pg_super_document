# check_indicator

## Location
[src/interfaces/ecpg/preproc/variable.c:465-497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/variable.c#L465-L497)

## Overview
Validates that a given variable is a valid indicator variable by checking its type for compliance with ECPG indicator variable requirements.

## Definition


## Detailed Description
The  function performs recursive validation of ECPG variable types to ensure they are suitable for use as indicator variables in embedded SQL contexts. Indicator variables in ECPG must have integer types to properly indicate NULL values or error conditions. The function traverses complex data structures (structs, unions, arrays) recursively to validate each component.

For basic integer types, the function simply allows them. For composite types (structs and unions), it recursively validates each member. For array types, it validates the element type. Any other type results in a parse error.

## Parameters / Member Variables
- : Pointer to ECPGtype structure representing the variable to validate

## Dependencies
- Functions called/Symbols referenced:
  - ECPGtype (struct type)
  - ECPGstruct_member (struct type) 
  - ECPGt_short, ECPGt_int, ECPGt_long, ECPGt_long_long (enum values)
  - ECPGt_unsigned_short, ECPGt_unsigned_int, ECPGt_unsigned_long, ECPGt_unsigned_long_long (enum values)
  - ECPGt_struct, ECPGt_union, ECPGt_array (enum values)
  - mmerror (for error reporting)
  - PARSE_ERROR, ET_ERROR (error constants)

- Called from (representative examples):
  - Self-recursive calls for struct members and array elements

## Notes and Other Information
- This function is part of the ECPG (Embedded SQL in C) preprocessor infrastructure
- Located in src/interfaces/ecpg/preproc/variable.c:465-497
- Performs recursive validation for composite data structures
- Essential for ensuring type safety in embedded SQL indicator variable usage
- Generates parse errors for invalid indicator variable types