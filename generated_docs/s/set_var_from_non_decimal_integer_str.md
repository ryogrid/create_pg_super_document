# set_var_from_non_decimal_integer_str

## Location
src/backend/utils/adt/numeric.c: 7258 - 7435

## Overview
A static function that parses non-decimal integer strings (binary, octal, or hexadecimal) and converts them into a NumericVar structure.

## Definition
```c
static bool set_var_from_non_decimal_integer_str(const char *str, const char *cp, int sign,
                                                int base, NumericVar *dest,
                                                const char **endptr, Node *escontext)
```

## Detailed Description
The `set_var_from_non_decimal_integer_str` function handles parsing of non-decimal integer representations in binary (base 2), octal (base 8), or hexadecimal (base 16) formats. It assumes that the sign and base prefix (e.g., "0x", "0b", "0o") have already been parsed by the caller. The function uses a sophisticated chunking algorithm that processes digits in groups that fit within int64 limits to handle arbitrarily large numbers while maintaining precision. It supports underscore digit separators for readability and includes comprehensive overflow detection and error handling.

## Parameters / Member Variables
- `str`: The original string for error reporting purposes  
- `cp`: Pointer to the first digit character to parse (after sign/prefix)
- `sign`: The numeric sign (NUMERIC_POS or NUMERIC_NEG) determined by caller
- `base`: The numeric base (2, 8, or 16)
- `dest`: Pointer to the NumericVar structure to store the parsed result
- `endptr`: Returns the position after the last parsed character
- `escontext`: Error handling context for soft error handling (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - init_var
  - zero_var
  - PG_INT64_MAX
  - int64_to_numericvar
  - mul_var
  - add_var
  - NUMERIC_WEIGHT_MAX
  - xdigit_value
  - free_var
  - ereturn
- Called from (representative examples):
  - NUMERIC_CAN_BE_SHORT
  - numeric_in

## Notes and Other Information
This function implements a chunked parsing algorithm to handle very large integers without losing precision. The algorithm processes digits in groups that fit within int64 range, converting each group and then using numeric arithmetic to combine them. This approach allows PostgreSQL to handle arbitrarily large non-decimal integers while maintaining exact precision. The function includes overflow protection by checking the numeric weight against NUMERIC_WEIGHT_MAX. The support for underscores as digit separators aligns with modern programming language standards for numeric literal readability.