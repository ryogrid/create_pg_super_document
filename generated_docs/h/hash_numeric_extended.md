# hash_numeric_extended

## Location
[src/backend/utils/adt/numeric.c:2793-2863](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L2793-L2863)

## Overview
Computes a seeded 64-bit hash value for PostgreSQL numeric data types, providing enhanced hash distribution for advanced hash-based operations.

## Definition

```c
Datum
hash_numeric_extended(PG_FUNCTION_ARGS)
```
## Detailed Description
This function generates a 64-bit hash value for numeric types using a provided seed value, extending the functionality of hash_numeric. It implements the same normalization logic as hash_numeric to ensure numerically equivalent values produce identical hash values, but returns a 64-bit result and incorporates a seed for better hash distribution in advanced scenarios.

The function is particularly useful for hash-based operations that benefit from larger hash spaces or require seeded hashing for security or distribution purposes. Like hash_numeric, it strips leading and trailing zeros and excludes scale from the hash calculation while incorporating the weight via XOR.

## Parameters / Member Variables
-  (PG_GETARG_NUMERIC(0)): The numeric value to be hashed
-  (PG_GETARG_INT64(1)): A 64-bit seed value to influence the hash calculation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC (parameter extraction)
  - PG_GETARG_INT64 (seed extraction)
  - NUMERIC_IS_SPECIAL (special value detection)
  - NUMERIC_WEIGHT (decimal point position)
  - NUMERIC_DIGITS (digit array access)
  - NUMERIC_NDIGITS (digit count)
  - [hash_any_extended](hash_any_extended.md) (seeded binary hash function)
  - [UInt64GetDatum](../U/UInt64GetDatum.md)/DatumGetUInt64 (64-bit datum conversion)
  - PG_RETURN_UINT64/PG_RETURN_DATUM (return value macros)
- Called from (representative examples):
  - [JsonbHashScalarValueExtended](../J/JsonbHashScalarValueExtended.md) (extended JSONB numeric hashing)

## Notes and Other Information
- Returns seed value for special values (NaN, infinity) to maintain consistency
- Returns seed-1 for zero values regardless of their representation
- Uses hash_any_extended instead of hash_any to incorporate the seed
- Maintains same normalization logic as hash_numeric for consistent equality semantics
- The weight is XORed with the digit hash result after datum conversion
- Provides 64-bit hash space for improved distribution in large hash tables
- Used in advanced hash operations requiring seeded or extended hash values

## Simplified Source

```c
Datum hash_numeric_extended(PG_FUNCTION_ARGS) {
    Numeric key = PG_GETARG_NUMERIC(0);
    uint64 seed = PG_GETARG_INT64(1);

    // Handle special values (NaN, infinity)
    if (NUMERIC_IS_SPECIAL(key))
        PG_RETURN_UINT64(seed);

    // Get numeric components and normalize
    int weight = NUMERIC_WEIGHT(key);
    NumericDigit *digits = NUMERIC_DIGITS(key);
    int ndigits = NUMERIC_NDIGITS(key);

    // Skip leading zeros
    int start_offset = 0;
    for (int i = 0; i < ndigits; i++) {
        if (digits[i] != 0) break;
        start_offset++;
        weight--;
    }

    // Handle all-zero number
    if (ndigits == start_offset)
        PG_RETURN_UINT64(seed - 1);

    // Skip trailing zeros
    int end_offset = 0;
    for (int i = ndigits - 1; i >= 0; i--) {
        if (digits[i] != 0) break;
        end_offset++;
    }

    // Hash the significant digits with seed
    int hash_len = ndigits - start_offset - end_offset;
    Datum digit_hash = hash_any_extended((unsigned char *) (digits + start_offset),
                                        hash_len * sizeof(NumericDigit), seed);

    // Mix in the weight and return as 64-bit value
    Datum result = UInt64GetDatum(DatumGetUInt64(digit_hash) ^ weight);
    PG_RETURN_DATUM(result);
}
```