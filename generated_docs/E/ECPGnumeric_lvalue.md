# ECPGnumeric_lvalue

## Location
src/interfaces/ecpg/preproc/descriptor.c: 46 - 75

## Overview
Validates that a variable has a numeric type and outputs its name to the generated code, used in ECPG descriptor operations that require numeric left-values.

## Definition


## Detailed Description
This function performs type validation for variables that must be numeric left-values in ECPG descriptor operations. It looks up the variable by name, checks if its type is one of the supported numeric types, and either outputs the variable name to the generated code stream or reports a parse error if the type is incompatible.

The function supports various integer types including signed and unsigned variants, as well as const qualified types. If the variable is not of a numeric type, it generates a parse error indicating the type requirement violation.

## Parameters / Member Variables
- : The name of the variable to validate and potentially output

## Dependencies
- Functions called/Symbols referenced:
  - find_variable (variable lookup function)
  - fputs (outputs name to base_yyout stream)
  - mmerror (error reporting function)
  - ECPGt_short, ECPGt_int, ECPGt_long, ECPGt_long_long (signed integer types)
  - ECPGt_unsigned_short, ECPGt_unsigned_int, ECPGt_unsigned_long, ECPGt_unsigned_long_long (unsigned integer types)
  - ECPGt_const (const qualified type)
  - PARSE_ERROR, ET_ERROR (error reporting constants)
- Called from (representative examples):
  - output_get_descr_header
  - output_set_descr_header

## Notes and Other Information
- Static function with file-local scope in descriptor.c
- Validates numeric types for descriptor operations requiring left-value expressions
- Uses base_yyout stream for code generation output
- Supports both signed and unsigned integer types of various sizes
- The const qualifier (ECPGt_const) is also accepted as a valid numeric type
- Part of the ECPG preprocessor's type safety system for SQL descriptor operations