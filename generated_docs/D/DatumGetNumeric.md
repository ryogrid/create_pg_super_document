# DatumGetNumeric

## Location
[src/include/utils/numeric.h:61-66](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/numeric.h#L61-L66)

## Overview
DatumGetNumeric is an inline function that converts a Datum value to a Numeric pointer, properly handling TOAST decompression for numeric data types.

## Definition

```c
static inline Numeric
DatumGetNumeric(Datum X)
```
## Detailed Description
This function serves as a conversion utility in PostgreSQL's function manager (fmgr) interface to extract Numeric data from a Datum. It wraps the PG_DETOAST_DATUM macro to ensure that if the numeric value is stored in TOAST format (for large values), it gets properly detoasted before being cast to a Numeric pointer. This is essential for accessing numeric values that may be stored externally due to PostgreSQL's TOAST (The Oversized Attribute Storage Technique) mechanism.

## Parameters / Member Variables
- : A Datum value containing numeric data that needs to be converted to a Numeric pointer

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM (macro for handling TOAST decompression)
  - [Numeric](../N/Numeric.md) (data type)
- Called from (representative examples):
  - [extract_date](../e/extract_date.md)
  - [numeric_absolute](../n/numeric_absolute.md)
  - [numeric_half_rounded](../n/numeric_half_rounded.md)
  - [numeric_truncated_divide](../n/numeric_truncated_divide.md)
  - [pg_size_bytes](../p/pg_size_bytes.md)
  - [numeric_to_number](../n/numeric_to_number.md)
  - [numeric_to_char](../n/numeric_to_char.md)
  - [jsonb_in_scalar](../j/jsonb_in_scalar.md)
  - [datum_to_jsonb_internal](../d/datum_to_jsonb_internal.md)
  - [numeric_fast_cmp](../n/numeric_fast_cmp.md)
  - [timestamp_part_common](../t/timestamp_part_common.md)
  - PG_GETARG_NUMERIC

## Notes and Other Information
- This is an inline function defined in src/include/utils/numeric.h for performance optimization
- Part of the fmgr interface macros used throughout PostgreSQL for type conversions
- Handles both regular and TOAST-compressed numeric values transparently
- Essential for any function that needs to work with Numeric data passed as Datum values
- The detoasting ensures that large numeric values stored externally are properly accessible

## Simplified Source

```c
static inline Numeric
DatumGetNumeric(Datum X)
{
    // Convert Datum to Numeric, handling TOAST decompression
    return (Numeric) PG_DETOAST_DATUM(X);
}
```