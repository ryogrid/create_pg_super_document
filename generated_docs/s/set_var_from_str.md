# set_var_from_str

## Location
[src/interfaces/ecpg/pgtypeslib/numeric.c:78-225](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/numeric.c#L78-L225)

## Overview
A comprehensive string-to-numeric parsing function that converts textual numeric representations into PostgreSQL's internal NumericVar format, handling various formats including scientific notation and digit separators.

## Definition
```c
static bool
set_var_from_str(const char *str, const char *cp,
                 NumericVar *dest, const char **endptr,
                 Node *escontext)
```

## Detailed Description
The `set_var_from_str` function is a sophisticated parser that handles the conversion of string representations of numbers into PostgreSQL's internal NumericVar format. This function supports a wide range of numeric formats including integers, decimals, scientific notation (with 'e' or 'E'), and numbers with underscore digit separators for improved readability. The parsing process involves two main phases: first extracting decimal digits and determining the decimal weight, then converting to PostgreSQL's NBASE representation. The function provides comprehensive error handling and can work with PostgreSQL's soft error reporting system through the escontext parameter.

## Parameters / Member Variables
- `str`: The original string for error reporting purposes
- `cp`: The actual parsing start position (typically after skipping leading spaces)
- `dest`: Pointer to the NumericVar structure that will receive the parsed numeric value
- `endptr`: Returns the position after the last parsed character
- `escontext`: Error context for soft error reporting (can be NULL for traditional error throwing)

## Dependencies
- Functions called/Symbols referenced:
  - [alloc_var](../a/alloc_var.md) (allocates digit buffer for the result)
  - [strip_var](strip_var.md) (normalizes the result by removing leading/trailing zeros)
  - [palloc](../p/palloc.md) (allocates temporary buffer for decimal digits)
  - [pfree](../p/pfree.md) (frees temporary buffer)
  - ereturn (soft error reporting)
- Called from (representative examples):
  - NUMERIC_CAN_BE_SHORT
  - [numeric_in](../n/numeric_in.md) (main numeric input function)
  - [float8_numeric](../f/float8_numeric.md) (float to numeric conversion)
  - [float4_numeric](../f/float4_numeric.md) (float to numeric conversion)
  - [PGTYPESnumeric_from_asc](../P/PGTYPESnumeric_from_asc.md) (ECPG library)

## Notes and Other Information
- Supports scientific notation with both positive and negative exponents
- Handles underscore separators within numbers (but not after decimal points or before digits)
- Uses a two-phase parsing approach: decimal extraction followed by NBASE conversion
- Supports both traditional error throwing and soft error reporting via escontext
- The function does not handle leading or trailing whitespace - this must be done by the caller
- Returns both success/failure status and the end position for further parsing by callers
- Implements strict validation to prevent various forms of malformed input
- The temporary decimal digit buffer includes padding for proper alignment during NBASE conversion

## Simplified Source

```c
static bool set_var_from_str(const char *str, const char *cp,
                            NumericVar *dest, const char **endptr,
                            Node *escontext)
{
    bool have_dp = false;        // Found decimal point
    int sign = NUMERIC_POS;      // Number sign
    int dweight = -1;            // Decimal weight (digits before decimal)
    int dscale = 0;              // Decimal scale (digits after decimal)
    unsigned char *decdigits;    // Temporary decimal digit buffer
    int i, ddigits, weight, ndigits, offset;
    NumericDigit *digits;

    // Parse optional sign
    if (*cp == '+') {
        cp++;
    } else if (*cp == '-') {
        sign = NUMERIC_NEG;
        cp++;
    }

    // Check for leading decimal point
    if (*cp == '.') {
        have_dp = true;
        cp++;
    }

    if (!isdigit(*cp))
        goto invalid_syntax;

    // Allocate buffer for decimal digits with padding
    decdigits = (unsigned char *) palloc(strlen(cp) + DEC_DIGITS * 2);
    memset(decdigits, 0, DEC_DIGITS);
    i = DEC_DIGITS;

    // Parse main number digits
    while (*cp) {
        if (isdigit(*cp)) {
            decdigits[i++] = *cp++ - '0';
            if (!have_dp)
                dweight++;      // Count digits before decimal
            else
                dscale++;       // Count digits after decimal
        }
        else if (*cp == '.') {
            if (have_dp) goto invalid_syntax;  // Multiple decimal points
            have_dp = true;
            cp++;
            if (*cp == '_') goto invalid_syntax;  // No underscore after decimal
        }
        else if (*cp == '_') {
            // Skip underscore separator, ensure followed by digit
            cp++;
            if (!isdigit(*cp)) goto invalid_syntax;
        }
        else
            break;  // End of number
    }

    ddigits = i - DEC_DIGITS;

    // Handle scientific notation exponent
    if (*cp == 'e' || *cp == 'E') {
        int64 exponent = 0;
        bool neg = false;

        cp++;
        if (*cp == '+') {
            cp++;
        } else if (*cp == '-') {
            neg = true;
            cp++;
        }

        if (!isdigit(*cp)) goto invalid_syntax;

        // Parse exponent digits
        while (*cp && isdigit(*cp)) {
            exponent = exponent * 10 + (*cp++ - '0');
            if (exponent > PG_INT32_MAX / 2)
                goto out_of_range;
        }

        if (neg) exponent = -exponent;

        // Apply exponent to weight and scale
        dweight += (int) exponent;
        dscale -= (int) exponent;
        if (dscale < 0) dscale = 0;
    }

    // Convert decimal representation to NBASE format
    if (dweight >= 0)
        weight = (dweight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1;
    else
        weight = -((-dweight - 1) / DEC_DIGITS + 1);

    offset = (weight + 1) * DEC_DIGITS - (dweight + 1);
    ndigits = (ddigits + offset + DEC_DIGITS - 1) / DEC_DIGITS;

    // Allocate and populate result
    alloc_var(dest, ndigits);
    dest->sign = sign;
    dest->weight = weight;
    dest->dscale = dscale;

    // Convert decimal digits to NBASE digits
    i = DEC_DIGITS - offset;
    digits = dest->digits;
    while (ndigits-- > 0) {
        // Pack DEC_DIGITS decimal digits into one NBASE digit
        *digits++ = ((decdigits[i] * 10 + decdigits[i + 1]) * 10 +
                     decdigits[i + 2]) * 10 + decdigits[i + 3];
        i += DEC_DIGITS;
    }

    pfree(decdigits);
    strip_var(dest);  // Remove leading/trailing zeros
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