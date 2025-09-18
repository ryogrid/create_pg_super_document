# lookup_var_attr_stats

## Location
src/backend/statistics/extended_stats.c: 693 - 761

## Overview
Constructs a VacAttrStats array for extended statistics by matching requested attributes and expressions with available column statistics, ensuring all required data is present before building extended statistics.

## Definition


## Detailed Description
The lookup_var_attr_stats function serves as a critical bridge in PostgreSQL's extended statistics system, responsible for assembling the necessary VacAttrStats structures needed to compute extended statistics. It takes a collection of available column statistics and filters/organizes them according to the specific attributes and expressions required for a particular extended statistics object.

The function operates in two phases:
1. **Column Lookup Phase**: Iterates through the requested attribute numbers (from the attrs Bitmapset) and finds the corresponding VacAttrStats from the available vacatts array. If any required column statistics are missing, the function immediately returns NULL, indicating that extended statistics cannot be computed.

2. **Expression Handling Phase**: For each expression in the exprs list, it calls examine_attribute to create appropriate VacAttrStats structures. These expressions represent computed values that are part of the extended statistics definition.

The function implements a strict "all-or-nothing" policy - if statistics for any required column or expression are unavailable, it returns NULL rather than attempting to build partial extended statistics. This ensures consistency and prevents misleading statistical information.

## Parameters / Member Variables
- : The relation for which extended statistics are being built
- : Bitmapset containing the attribute numbers of columns needed for the extended statistics
- : List of expression nodes that are part of the extended statistics definition
- : Number of available VacAttrStats structures in the vacatts array
- : Array of available VacAttrStats structures from the relation's analysis

## Dependencies
- Functions called/Symbols referenced:
  - VacAttrStats (structure manipulation)
  - [bms_num_members](../b/bms_num_members.md) (count members in Bitmapset)
  - [bms_next_member](../b/bms_next_member.md) (iterate through Bitmapset)
  - [examine_attribute](../e/examine_attribute.md) (analyze expressions for statistics)
  - [palloc](../p/palloc.md), pfree (memory management)
- Called from (representative examples):
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md) (main extended statistics builder)
  - [ComputeExtStatisticsRows](../C/ComputeExtStatisticsRows.md) (statistics computation)

## Notes and Other Information
- Returns NULL if any required column statistics are missing, implementing an all-or-nothing approach
- The function includes a workaround for tuple descriptor handling with expressions (noted by XXX comment)
- Expression statistics inherit the tuple descriptor from the first available column statistic
- Memory management includes cleanup of allocated stats array on failure
- The function is part of the extended statistics infrastructure that improves query planning for correlated columns and complex expressions
- The Bitmapset iteration uses a standard PostgreSQL pattern with bms_next_member starting from -1
- Each expression is processed through examine_attribute, which may call examine_expression internally for proper expression analysis