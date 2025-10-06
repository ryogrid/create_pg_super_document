# numeric_abbrev_convert_var

## Location
[src/backend/utils/adt/numeric.c:2281-2336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L2281-L2336)

## Overview
Converts a NumericVar into an abbreviated representation for optimized sorting operations, encoding the most significant parts of the numeric value into a 64-bit integer.

## Definition
```c
static Datum numeric_abbrev_convert_var(const NumericVar *var, NumericSortSupport *nss)
```

## Detailed Description
The `numeric_abbrev_convert_var` function creates an abbreviated representation of a PostgreSQL numeric value stored in a NumericVar structure. This abbreviation is designed for fast comparison operations during sorting while preserving the correct ordering relationship between values.

The function performs several key operations:
1. Handles special cases (zero values, very small/large numbers)
2. Encodes the weight (exponent) and up to 4 most significant digits into a 64-bit value
3. Negates positive values to handle the comparison logic correctly
4. Updates cardinality estimation statistics if enabled
5. Returns the abbreviated value as a Datum

The abbreviation scheme uses bit packing to store:
- Weight (exponent + 44) in the upper 8 bits
- Up to 4 digits in descending significance: digits[0] to digits[3]

## Parameters / Member Variables
- `var`: Pointer to the NumericVar structure containing the numeric value to abbreviate
- `nss`: Pointer to NumericSortSupport structure containing sort context and cardinality estimation state

## Dependencies
- Functions called/Symbols referenced:
  - PG_INT64_MAX (maximum 64-bit integer constant)
  - NUMERIC_POS (positive sign constant)
  - [addHyperLogLog](../a/addHyperLogLog.md) (cardinality estimation function)
  - [hash_uint32](../h/hash_uint32.md) (32-bit hash function for cardinality estimation)
  - [DatumGetUInt32](../D/DatumGetUInt32.md) (extracts uint32 from Datum)
  - NumericAbbrevGetDatum (converts abbreviation to Datum)
  - NUMERIC_ABBREV_BITS (abbreviation bit manipulation)
- Called from (representative examples):
  - [numeric_abbrev_convert](numeric_abbrev_convert.md) (main abbreviation conversion entry point)
  - NUMERIC_CAN_BE_SHORT (abbreviation feasibility checking)

## Notes and Other Information
- Uses a fallthrough switch statement to handle variable numbers of digits efficiently
- Positive values are negated in the abbreviation to ensure proper comparison ordering
- Weight range handling: values with weight < -44 become 0, weight > 83 become PG_INT64_MAX
- Cardinality estimation uses HyperLogLog algorithm when enabled to track distinct value distribution
- The abbreviation preserves ordering but may have collisions (different values with same abbreviation)
- Part of PostgreSQL's sort support optimization system for improved numeric sorting performance

## Simplified Source

```c
static Datum
numeric_abbrev_convert_var(const NumericVar *var, NumericSortSupport *nss)
{
    int ndigits = var->ndigits;
    int weight = var->weight;
    int64 result;

    // Handle special cases
    if (ndigits == 0 || weight < -44)
    {
        result = 0;
    }
    else if (weight > 83)
    {
        result = PG_INT64_MAX;
    }
    else
    {
        // Encode weight in upper bits
        result = ((int64) (weight + 44) << 56);

        // Pack up to 4 digits in descending significance
        switch (ndigits)
        {
            default:
                result |= ((int64) var->digits[3]);
                /* FALLTHROUGH */
            case 3:
                result |= ((int64) var->digits[2]) << 14;
                /* FALLTHROUGH */
            case 2:
                result |= ((int64) var->digits[1]) << 28;
                /* FALLTHROUGH */
            case 1:
                result |= ((int64) var->digits[0]) << 42;
                break;
        }
    }

    // Negate positive values for correct comparison ordering
    if (var->sign == NUMERIC_POS)
        result = -result;

    // Update cardinality estimation if enabled
    if (nss->estimating)
    {
        uint32 tmp = ((uint32) result ^ (uint32) ((uint64) result >> 32));
        addHyperLogLog(&nss->abbr_card, DatumGetUInt32(hash_uint32(tmp)));
    }

    return NumericAbbrevGetDatum(result);
}
```