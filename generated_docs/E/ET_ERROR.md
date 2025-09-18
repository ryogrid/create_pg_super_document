# ET_ERROR

## Location
src/interfaces/ecpg/preproc/type.h: 219 - 221

## Overview
ET_ERROR is an enumeration value in the errortype enum that represents an error condition in the ECPG (Embedded SQL in C) preprocessor's error handling system.

## Definition
```c
enum errortype
{
    ET_WARNING, ET_ERROR
};
```

## Detailed Description
ET_ERROR is one of two enumeration values in the errortype enum used by the ECPG preprocessor to classify different severity levels of diagnostic messages. When ET_ERROR is passed to error reporting functions like mmerror(), it indicates that the condition represents a fatal error that should cause compilation to fail. This is used throughout the ECPG preprocessor to report various parsing errors, type mismatches, invalid syntax, and other conditions that prevent successful code generation.

## Parameters / Member Variables
- N/A (This is an enumeration constant)

## Dependencies
- Functions called/Symbols referenced:
  - N/A (enumeration constant)
- Called from (representative examples):
  - [ECPGnumeric_lvalue](ECPGnumeric_lvalue.md) (in src/interfaces/ecpg/preproc/descriptor.c:64)
  - [filtered_base_yylex](../f/filtered_base_yylex.md) (in src/interfaces/ecpg/preproc/parser.c:190, 198)
  - [get_type](../g/get_type.md) (in src/interfaces/ecpg/preproc/type.c:214)
  - [ECPGdump_a_type](ECPGdump_a_type.md) (in src/interfaces/ecpg/preproc/type.c:263, 277, 291)
  - [ECPGfree_type](ECPGfree_type.md) (in src/interfaces/ecpg/preproc/type.c:685)
  - [get_dtype](../g/get_dtype.md) (in src/interfaces/ecpg/preproc/type.c:744)
  - [check_indicator](../c/check_indicator.md) (in src/interfaces/ecpg/preproc/variable.c:492)

## Notes and Other Information
- Defined in src/interfaces/ecpg/preproc/type.h:219
- Used as a parameter to mmerror() and vmmerror() functions to indicate error severity
- Distinguishes fatal errors from warnings (ET_WARNING) in the ECPG preprocessor
- Common usage pattern: mmerror(PARSE_ERROR, ET_ERROR, "error message", ...)
- Part of the ECPG preprocessor's comprehensive error reporting and diagnostic system
- When used, typically causes the preprocessor to exit with failure status