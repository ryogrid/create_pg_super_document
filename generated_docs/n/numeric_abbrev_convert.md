# numeric_abbrev_convert

## Location
[src/backend/utils/adt/numeric.c:2062-2123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L2062-L2123)

## Overview
Converts numeric values to abbreviated forms for optimized sorting, handling special values (NaN, infinity) and efficiently managing memory for packed datums.

## Definition

```c
static Datum
numeric_abbrev_convert(Datum original_datum, SortSupport ssup)
```
## Detailed Description
The `numeric_abbrev_convert` function is a key component of PostgreSQL's numeric sort optimization system. It converts full numeric values into abbreviated representations that can be compared much faster than the original values.

The function handles several important aspects:

1. **Memory Management**: Efficiently handles packed datums without unnecessary palloc/pfree cycles by using a pre-allocated buffer for short values
2. **Special Value Handling**: Properly converts special numeric values (positive infinity, negative infinity, NaN) to their abbreviated constants
3. **Detoasting**: Safely handles toasted (compressed/external) numeric values
4. **Input Tracking**: Maintains count statistics for the abbreviation process

For regular numeric values, it delegates to `numeric_abbrev_convert_var` after converting the numeric to NumericVar format. The function ensures no memory leaks by carefully managing detoasted values.

## Parameters / Member Variables
- `original_datum`: The original numeric Datum to be abbreviated  
- `ssup`: SortSupport structure containing abbreviation context and buffer

## Dependencies
- Functions called/Symbols referenced:
  - PG_DETOAST_DATUM_PACKED (datum detoasting)
  - [init_var_from_num](../i/init_var_from_num.md) (numeric to NumericVar conversion)
  - [numeric_abbrev_convert_var](numeric_abbrev_convert_var.md) (actual abbreviation logic)
  - VARATT_IS_SHORT, VARSIZE_SHORT, SET_VARSIZE (variable-length attribute macros)
  - NUMERIC_IS_SPECIAL, NUMERIC_IS_PINF, NUMERIC_IS_NINF (special value tests)
  - [pfree](../p/pfree.md) (memory deallocation)
- Called from (representative examples):
  - [numeric_sortsupport](numeric_sortsupport.md) (as abbreviation converter callback)

## Notes and Other Information
- This is a static function internal to numeric.c module
- Critical for performance: must not leak memory due to frequent usage during sorting
- Uses a reusable buffer strategy to avoid allocation overhead for short values
- Special values get fixed abbreviation constants: NUMERIC_ABBREV_PINF, NUMERIC_ABBREV_NINF, NUMERIC_ABBREV_NAN
- Handles both packed and unpacked numeric representations efficiently
- Input count tracking supports the abbreviation abort mechanism