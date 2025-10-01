# set_var_from_non_decimal_integer_str

## Location
[src/backend/utils/adt/numeric.c:7258-7435](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L7258-L7435)

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
  - [zero_var](../z/zero_var.md)
  - PG_INT64_MAX
  - [int64_to_numericvar](../i/int64_to_numericvar.md)
  - [mul_var](../m/mul_var.md)
  - [add_var](../a/add_var.md)
  - NUMERIC_WEIGHT_MAX
  - [xdigit_value](../x/xdigit_value.md)
  - [free_var](../f/free_var.md)
  - ereturn
- Called from (representative examples):
  - NUMERIC_CAN_BE_SHORT
  - [numeric_in](../n/numeric_in.md)

## Notes and Other Information
This function implements a chunked parsing algorithm to handle very large integers without losing precision. The algorithm processes digits in groups that fit within int64 range, converting each group and then using numeric arithmetic to combine them. This approach allows PostgreSQL to handle arbitrarily large non-decimal integers while maintaining exact precision. The function includes overflow protection by checking the numeric weight against NUMERIC_WEIGHT_MAX. The support for underscores as digit separators aligns with modern programming language standards for numeric literal readability.

## Simplified Source

```c
static bool set_var_from_non_decimal_integer_str(const char *str, const char *cp, int sign,
                                                int base, NumericVar *dest,
                                                const char **endptr, Node *escontext)
{
    const char *firstdigit = cp;
    int64 tmp = 0;          // Current group value
    int64 mul = 1;          // Current group multiplier
    NumericVar tmp_var;

    init_var(&tmp_var);
    zero_var(dest);

    // Process digits in groups that fit in int64 to handle large numbers
    while (*cp) {
        if (is_valid_digit_for_base(*cp, base)) {
            // Check if adding another digit would overflow int64
            if (mul > PG_INT64_MAX / base) {
                // Add current group to result
                int64_to_numericvar(mul, &tmp_var);
                mul_var(dest, &tmp_var, dest, 0);
                int64_to_numericvar(tmp, &tmp_var);
                add_var(dest, &tmp_var, dest);

                // Check for numeric overflow
                if (dest->weight > NUMERIC_WEIGHT_MAX)
                    goto out_of_range;

                // Start new group
                tmp = 0;
                mul = 1;
            }

            // Add digit to current group
            tmp = tmp * base + digit_value(*cp++);
            mul = mul * base;
        }
        else if (*cp == '_') {
            // Skip underscore separators, ensure followed by valid digit
            cp++;
            if (!is_valid_digit_for_base(*cp, base))
                goto invalid_syntax;
        }
        else
            break;  // End of number
    }

    // Validate we got at least one digit
    if (cp == firstdigit)
        goto invalid_syntax;

    // Add final group to result
    int64_to_numericvar(mul, &tmp_var);
    mul_var(dest, &tmp_var, dest, 0);
    int64_to_numericvar(tmp, &tmp_var);
    add_var(dest, &tmp_var, dest);

    if (dest->weight > NUMERIC_WEIGHT_MAX)
        goto out_of_range;

    dest->sign = sign;
    free_var(&tmp_var);
    *endptr = cp;
    return true;

out_of_range:
    ereturn(escontext, false,
            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
             errmsg("value overflows numeric format")));

invalid_syntax:
    ereturn(escontext, false,
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
             errmsg("invalid input syntax for type %s: \"%s\"",
                    "numeric", str)));
}
```