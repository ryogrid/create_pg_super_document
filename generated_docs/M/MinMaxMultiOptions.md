# MinMaxMultiOptions

## Location
src/backend/access/brin/brin_minmax_multi.c: 121 - 125

## Overview
MinMaxMultiOptions is a storage structure for BRIN minmax-multi operator class reloptions (relation options) that configures how many values are stored per range in the index.

## Definition


## Detailed Description
MinMaxMultiOptions serves as the configuration structure for BRIN minmax-multi indexes, specifically controlling the number of values stored per block range. This structure follows PostgreSQL's varlena (variable-length array) convention for storing relation options. The minmax-multi operator class extends the basic minmax approach by allowing multiple min/max pairs per range, providing better selectivity for data with high cardinality within blocks.

## Parameters / Member Variables
- `vl_len_`: Standard varlena header containing the total length of the structure; this field should not be manipulated directly but is managed by PostgreSQL's varlena infrastructure
- `valuesPerRange`: Integer specifying how many minimum/maximum value pairs to maintain per block range; higher values provide better selectivity at the cost of increased storage and maintenance overhead

## Dependencies
- Used by functions:
  - MinMaxMultiGetValuesPerRange
  - [brin_minmax_multi_get_values](../b/brin_minmax_multi_get_values.md)
  - [brin_minmax_multi_add_value](../b/brin_minmax_multi_add_value.md)
  - [brin_minmax_multi_options](../b/brin_minmax_multi_options.md)

## Notes and Other Information
- This structure implements the reloptions (relation options) interface for BRIN minmax-multi indexes
- The valuesPerRange parameter allows tuning the trade-off between index selectivity and storage overhead
- Part of PostgreSQL's extensible BRIN indexing framework
- The varlena format ensures compatibility with PostgreSQL's variable-length data storage system
- Default and valid ranges for valuesPerRange are typically defined in the operator class implementation