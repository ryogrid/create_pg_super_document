# network_sortsupport

## Location
[src/backend/utils/adt/network.c:437-472](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network.c#L437-L472)

## Overview
PostgreSQL sort support strategy function that optimizes sorting operations for network address data types by providing specialized comparison and abbreviation mechanisms.

## Definition

```c
Datum
network_sortsupport(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements PostgreSQL's SortSupport strategy for network address types (inet/cidr). It configures optimized sorting mechanisms that can significantly improve performance for operations involving large-scale sorting of network addresses, such as ORDER BY clauses, index builds, and merge operations.

The function operates in two modes:

1. **Basic mode**: Sets up fast comparison using  as the primary comparator
2. **Abbreviation mode**: When abbreviation is supported, it enables advanced optimizations:
   - Uses abbreviated keys for faster initial comparisons
   - Maintains cardinality estimation using HyperLogLog algorithm
   - Provides fallback to full comparison when needed
   - Tracks input statistics to determine abbreviation effectiveness

The abbreviation system creates shorter representations of network addresses for faster comparison while maintaining correct sort order. This is particularly beneficial for datasets with high cardinality where the abbreviated keys can distinguish most values without requiring full comparison.

## Parameters / Member Variables
- Uses  - PostgreSQL standard function argument structure containing:
  - First argument (index 0):  structure to configure

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL argument extraction macro)
  -  (fast comparison function)
  -  (memory context management)
  -  (memory allocation)
  -  (cardinality estimation initialization)
  -  (unsigned datum comparator)
  -  (abbreviation key converter)
  -  (abbreviation abort handler)
  -  (PostgreSQL return macro)

- Data structures used:
  -  (sort support state tracking)
  -  (PostgreSQL sort support structure)

- Called from (representative examples):
  - No direct references found (likely registered as a PostgreSQL built-in function and called by the sort support system)

## Notes and Other Information
- Enhances sorting performance through specialized comparison routines and abbreviation keys
- Uses HyperLogLog algorithm for cardinality estimation to optimize abbreviation effectiveness
- Memory allocation occurs in the sort support memory context to ensure proper cleanup
- The abbreviation system dynamically adjusts based on input data characteristics
- Essential for high-performance sorting operations on network data types in PostgreSQL
- Part of PostgreSQL's advanced sort optimization infrastructure
- Located in 
- Registered in PostgreSQL's system catalogs for automatic use during sort operations

## Simplified Source

```c
Datum network_sortsupport(PG_FUNCTION_ARGS) {
    SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

    // Set basic fast comparator for network types
    ssup->comparator = network_fast_cmp;
    ssup->ssup_extra = NULL;

    // Enable abbreviation optimization if supported
    if (ssup->abbreviate) {
        // Allocate state tracking structure in sort context
        MemoryContext oldcontext = MemoryContextSwitchTo(ssup->ssup_cxt);

        network_sortsupport_state *uss = palloc(sizeof(network_sortsupport_state));
        uss->input_count = 0;
        uss->estimating = true;
        initHyperLogLog(&uss->abbr_card, 10);  // Track cardinality

        ssup->ssup_extra = uss;

        // Configure abbreviation functions
        ssup->comparator = ssup_datum_unsigned_cmp;
        ssup->abbrev_converter = network_abbrev_convert;
        ssup->abbrev_abort = network_abbrev_abort;
        ssup->abbrev_full_comparator = network_fast_cmp;

        MemoryContextSwitchTo(oldcontext);
    }

    PG_RETURN_VOID();
}
```