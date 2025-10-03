# brin_minmax_multi_options

## Location
[src/backend/access/brin/brin_minmax_multi.c:2954-2975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L2954-L2975)

## Overview
Define and register reloptions (relation options) for the BRIN minmax-multi operator class.

## Definition

```c
Datum
brin_minmax_multi_options(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the options handler for the BRIN minmax-multi operator class, allowing users to configure behavior parameters when creating BRIN indexes. Currently, it defines a single option 'values_per_range' that controls how many distinct values can be stored per range before the range gets collapsed.

The function initializes the local reloptions structure with the size of MinMaxMultiOptions and adds an integer option for 'values_per_range' with a default value, minimum of 8, and maximum of 256 values per range.

This is part of the PostgreSQL reloptions (relation options) framework that allows index methods to accept custom configuration parameters.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: local_relopts pointer - Structure for defining relation options
## Dependencies
- Functions called/Symbols referenced:
  - [init_local_reloptions](../i/init_local_reloptions.md)
  - [add_local_int_reloption](../a/add_local_int_reloption.md)
  - MINMAX_MULTI_DEFAULT_VALUES_PER_PAGE (constant)
  - [MinMaxMultiOptions](../M/MinMaxMultiOptions.md) (structure type)
  - PG_RETURN_VOID
- Called from (representative examples):
  - No direct references found (likely called through relation options framework)

## Notes and Other Information
- This function is part of the SQL-visible interface for configuring BRIN minmax-multi indexes
- The 'values_per_range' option directly affects index performance and storage efficiency
- Lower values_per_range means more precise ranges but larger index size
- Higher values_per_range means less precise ranges but smaller index size
- The option can be specified when creating a BRIN index: 
- Default value is defined by MINMAX_MULTI_DEFAULT_VALUES_PER_PAGE constant

## Simplified Source

```c
Datum brin_minmax_multi_options(PG_FUNCTION_ARGS) {
    local_relopts *relopts = (local_relopts *) PG_GETARG_POINTER(0);

    // Initialize the relation options structure
    init_local_reloptions(relopts, sizeof(MinMaxMultiOptions));

    // Add the 'values_per_range' option with default, min, and max values
    add_local_int_reloption(relopts, "values_per_range", "desc",
                            MINMAX_MULTI_DEFAULT_VALUES_PER_PAGE, 8, 256,
                            offsetof(MinMaxMultiOptions, valuesPerRange));

    PG_RETURN_VOID();
}
```