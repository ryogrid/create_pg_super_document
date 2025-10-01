# get_rel_data_width

## Location
[src/backend/optimizer/util/plancat.c:1185-1226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L1185-L1226)

## Overview
Estimates the average width of the data part of a relation's tuples, providing essential information for query optimization and cost estimation.

## Definition

```c
int32
get_rel_data_width(Relation rel, int32 *attr_widths)
```
## Detailed Description
The  function calculates an estimate of the average tuple width for a given relation by iterating through all non-dropped attributes and summing their individual widths. The function serves as a critical component in PostgreSQL's query optimizer for cost estimation purposes.

The function implements a caching mechanism through the optional  parameter, which can store previously computed attribute widths to avoid redundant calculations. For each attribute, it first attempts to use cached data, then falls back to statistical information via , and finally uses type-based defaults via  if no statistics are available.

The function explicitly ignores dropped columns since information about them is not readily available, and treating them as zero-width is often acceptable since they are typically mostly NULLs.

## Parameters / Member Variables
- : The relation (table/index) for which to estimate tuple width
- : Optional pointer to a cache array for storing/retrieving previously computed attribute widths (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfAttributes
  - [get_attavgwidth](get_attavgwidth.md)  
  - [get_typavgwidth](get_typavgwidth.md)
  - [clamp_width_est](../c/clamp_width_est.md)
- Called from (representative examples):
  - [table_block_relation_estimate_size](../t/table_block_relation_estimate_size.md)
  - [estimate_rel_size](../e/estimate_rel_size.md)
  - [get_relation_data_width](get_relation_data_width.md)

## Notes and Other Information
- The function matches the logic in  in costsize.c for consistency
- Dropped columns are intentionally ignored due to lack of available information
- The final result is clamped to prevent integer overflow using 
- Uses 1-based indexing for attributes following PostgreSQL conventions
- Returns int32 but uses int64 internally to prevent overflow during calculation

## Simplified Source

```c
int32 get_rel_data_width(Relation rel, int32 *attr_widths) {
    int64 tuple_width = 0;

    // Iterate through all attributes in the relation
    for (int i = 1; i <= RelationGetNumberOfAttributes(rel); i++) {
        Form_pg_attribute att = TupleDescAttr(rel->rd_att, i - 1);
        int32 item_width;

        // Skip dropped columns
        if (att->attisdropped)
            continue;

        // Use cached width if available
        if (attr_widths != NULL && attr_widths[i] > 0) {
            tuple_width += attr_widths[i];
            continue;
        }

        // Get width from statistics or type defaults
        item_width = get_attavgwidth(RelationGetRelid(rel), i);
        if (item_width <= 0) {
            item_width = get_typavgwidth(att->atttypid, att->atttypmod);
            Assert(item_width > 0);
        }

        // Cache the computed width
        if (attr_widths != NULL)
            attr_widths[i] = item_width;

        tuple_width += item_width;
    }

    // Clamp result to prevent overflow
    return clamp_width_est(tuple_width);
}
```