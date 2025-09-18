# brin_minmax_multi_get_values

## Location
[src/backend/access/brin/brin_minmax_multi.c:2399-2411](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L2399-L2411)

## Overview
Retrieves the maximum number of values per range for BRIN minmax multi-column operations based on the configured options.

## Definition
```c
static int brin_minmax_multi_get_values(BrinDesc *bdesc, MinMaxMultiOptions *opts)
```

## Detailed Description
This static helper function serves as a simple wrapper that extracts the configured maximum number of values per range from the BRIN minmax multi-column options. It delegates the actual value retrieval to the `MinMaxMultiGetValuesPerRange` macro or function, which interprets the options structure to return the appropriate limit.

The function is part of the BRIN minmax multi-column infrastructure and is used internally to determine how many distinct values should be stored per range in the index. This configuration affects both storage efficiency and query performance - more values per range provide better selectivity but consume more storage space.

## Parameters / Member Variables
- `bdesc`: BRIN descriptor containing index metadata (unused in current implementation)
- `opts`: MinMaxMultiOptions structure containing configuration parameters for the minmax multi-column operations

## Dependencies
- Functions called/Symbols referenced:
  - MinMaxMultiGetValuesPerRange: Macro/function that extracts values per range from options
- Called from (representative examples):
  - [brin_minmax_multi_add_value](brin_minmax_multi_add_value.md): Used to determine range capacity limits during value insertion

## Notes and Other Information
- Function is declared as static, limiting its scope to the brin_minmax_multi.c file
- Currently the BrinDesc parameter is not used in the implementation
- Acts as an abstraction layer for accessing configuration values
- The returned integer represents the maximum number of distinct values that can be stored per range
- Part of the configuration management for BRIN minmax multi-column index optimization
- Simple wrapper function that could potentially be inlined for performance