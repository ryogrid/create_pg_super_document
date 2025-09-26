# PGLC_localeconv

## Location
[src/backend/utils/adt/pg_locale.c:547-757](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L547-L757)

## Overview
PGLC_localeconv returns a POSIX lconv struct containing locale-specific number and money formatting information for all categories, handling encoding conversion and caching for PostgreSQL's locale system.

## Definition

```c
struct lconv CurrentLocaleConv;
```
## Detailed Description
PGLC_localeconv is PostgreSQL's wrapper around the standard C library localeconv() function. It provides locale-specific formatting information for numeric and monetary values while handling the complexity of multiple locale categories, encoding conversion, and caching.

The function performs several critical operations:
- **Caching**: Uses static variables to cache results and avoid repeated expensive locale operations
- **Safe locale switching**: Temporarily switches between LC_NUMERIC and LC_MONETARY locales while preserving the original settings
- **Encoding conversion**: Converts locale-specific strings from their native encoding to PostgreSQL's database encoding
- **Memory management**: Uses strdup() for all string fields to ensure safe memory handling
- **Error handling**: Uses PostgreSQL's exception system (PG_TRY/PG_CATCH) to ensure cleanup on errors

The implementation is particularly complex on Windows, where LC_CTYPE must match LC_MONETARY/LC_NUMERIC to get consistent results, requiring additional locale switching.

## Parameters / Member Variables
This function takes no parameters but returns a pointer to a static struct lconv containing:

**Numeric formatting fields:**
- : Decimal point character for non-monetary values
- : Thousands separator for non-monetary values  
- : Grouping rules for non-monetary values

**Monetary formatting fields:**
- : International currency symbol
- : Local currency symbol
- : Decimal point for monetary values
- : Thousands separator for monetary values
- : Grouping rules for monetary values
- : Sign for positive monetary values
- : Sign for negative monetary values
- : Fractional digits for international currency
- : Fractional digits for local currency
- : Currency symbol position for positive values
- : Space separation for positive values
- : Currency symbol position for negative values
- : Space separation for negative values
- : Sign position for positive values
- : Sign position for negative values

## Dependencies
- Functions called/Symbols referenced:
  - [free_struct_lconv](../f/free_struct_lconv.md)
  - setlocale
  - [struct_lconv_is_valid](../s/struct_lconv_is_valid.md)
  - [pg_get_encoding_from_locale](../p/pg_get_encoding_from_locale.md)
  - [db_encoding_convert](../d/db_encoding_convert.md)
  - PG_TRY/PG_CATCH/PG_END_TRY
  - PG_SQL_ASCII
  - localeconv (standard C library)
  - strdup (standard C library)

- Called from (representative examples):
  - [cash_in](../c/cash_in.md) (src/backend/utils/adt/cash.c:189)
  - [cash_out](../c/cash_out.md) (src/backend/utils/adt/cash.c:403)
  - [cash_numeric](../c/cash_numeric.md) (src/backend/utils/adt/cash.c:1051)
  - [numeric_cash](../n/numeric_cash.md) (src/backend/utils/adt/cash.c:1110)
  - [NUM_prepare_locale](../N/NUM_prepare_locale.md) (src/backend/utils/adt/formatting.c:5296)

## Notes and Other Information
- **Thread Safety**: Uses static variables for caching, so not inherently thread-safe
- **Performance**: Expensive operation on first call, but subsequent calls return cached results
- **Platform Differences**: Has special handling for Windows locale behavior
- **Error Recovery**: Uses FATAL errors for locale restoration failures, as continuing with wrong locale settings would be dangerous
- **Memory Management**: All string fields in the returned struct are allocated with strdup() and managed by PostgreSQL's memory system
- **Encoding**: Automatically converts locale strings to the database encoding, using PG_SQL_ASCII as fallback for unknown encodings
- **Cache Invalidation**: Cache is invalidated and rebuilt when locale settings change