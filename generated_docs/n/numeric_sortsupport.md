# numeric_sortsupport

## Location
[src/backend/utils/adt/numeric.c:2021-2061](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L2021-L2061)

## Overview
Implements PostgreSQL's sort support strategy for numeric data types, providing optimized comparison operations and abbreviation support for faster sorting performance.

## Definition

```c
struct
		 */
		nss->buf = palloc(VARATT_SHORT_MAX + VARHDRSZ + 1);
```
## Detailed Description
The `numeric_sortsupport` function is PostgreSQL's sort support strategy routine for the numeric data type. It optimizes sorting operations by implementing an abbreviation strategy that can significantly improve performance, especially for large datasets.

The function sets up two levels of comparison:
1. **Fast comparison**: Uses `numeric_fast_cmp` as the primary comparator
2. **Abbreviation support**: When abbreviation is enabled, it configures abbreviated key conversion and comparison routines

The abbreviation strategy converts numeric values into abbreviated forms that fit into native integer types (int32 or int64), allowing for much faster comparisons. The abbreviated values are negated relative to the original to handle NaN values correctly (NaN gets the largest negative value since it sorts higher than other values).

The implementation includes adaptive abortion logic - if the abbreviation cardinality drops below 0.01% of the row count (indicating low effectiveness), the abbreviation process is abandoned to avoid overhead without benefit.

## Parameters / Member Variables
- **Function Arguments**: Uses `PG_FUNCTION_ARGS` macro to access SortSupport pointer
- **ssup**: SortSupport structure containing sort configuration and callbacks

## Dependencies
- Functions called/Symbols referenced:
  - [numeric_fast_cmp](numeric_fast_cmp.md) (primary comparator)
  - [numeric_cmp_abbrev](numeric_cmp_abbrev.md) (abbreviated comparator)
  - [numeric_abbrev_convert](numeric_abbrev_convert.md) (abbreviation converter)
  - [numeric_abbrev_abort](numeric_abbrev_abort.md) (abbreviation abort handler)
  - [initHyperLogLog](../i/initHyperLogLog.md) (for cardinality estimation)
  - [palloc](../p/palloc.md) (memory allocation)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory context management)
- Called from (representative examples):
  - PostgreSQL's sort support infrastructure (via function pointer)

## Notes and Other Information
- This is a PostgreSQL function callable from SQL (uses PG_FUNCTION_ARGS macro)
- Implements sophisticated abbreviation strategy with adaptive abortion based on cardinality analysis
- Uses HyperLogLog algorithm for efficient cardinality estimation during abbreviation
- Memory allocation occurs in the sort support context to ensure proper cleanup
- The abbreviation buffer is sized to handle unaligned packed values (VARATT_SHORT_MAX + VARHDRSZ + 1)
- Abbreviation effectiveness is monitored and can be dynamically disabled if not beneficial

## Simplified Source

```c
Datum
numeric_sortsupport(PG_FUNCTION_ARGS)
{
    SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

    // Set primary comparator
    ssup->comparator = numeric_fast_cmp;

    // Configure abbreviation support if requested
    if (ssup->abbreviate)
    {
        NumericSortSupport *nss;
        MemoryContext oldcontext = MemoryContextSwitchTo(ssup->ssup_cxt);

        // Allocate and initialize sort support structure
        nss = palloc(sizeof(NumericSortSupport));
        nss->buf = palloc(VARATT_SHORT_MAX + VARHDRSZ + 1);
        nss->input_count = 0;
        nss->estimating = true;
        initHyperLogLog(&nss->abbr_card, 10);

        // Set up abbreviation callbacks
        ssup->ssup_extra = nss;
        ssup->abbrev_full_comparator = ssup->comparator;
        ssup->comparator = numeric_cmp_abbrev;
        ssup->abbrev_converter = numeric_abbrev_convert;
        ssup->abbrev_abort = numeric_abbrev_abort;

        MemoryContextSwitchTo(oldcontext);
    }

    PG_RETURN_VOID();
}
```