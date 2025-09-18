# FindColsContext

## Location
src/backend/executor/nodeAgg.c: 360 - 365

## Overview
FindColsContext is a helper structure used during column reference analysis to track which columns are referenced within aggregate functions versus those referenced outside of aggregates.

## Definition


## Detailed Description
FindColsContext serves as a context structure for traversing expression trees to identify column references and categorize them based on their relationship to aggregate functions. The structure is used during the analysis phase of query planning to determine which columns are referenced within aggregate function calls (aggregated) versus those referenced in other contexts (unaggregated). This distinction is crucial for determining grouping requirements and optimizing aggregate execution. The is_aggref flag tracks the current traversal state, indicating whether the walker is currently inside an aggregate function reference.

## Parameters / Member Variables
- : Boolean flag indicating whether the current expression traversal is within an aggregate function reference
- : Bitmapset containing column numbers that are referenced within aggregate function calls
- : Bitmapset containing column numbers that are referenced outside of aggregate function contexts

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references - uses basic types)
- Called from (representative examples):
  - find_cols
  - find_cols_walker

## Notes and Other Information
- Used specifically for expression tree analysis in aggregate query processing
- Part of PostgreSQL's query planning infrastructure for aggregate optimization
- The bitmapsets efficiently track column references using bit positions corresponding to column numbers
- Essential for determining which columns must be available at different stages of aggregate processing
- Helps distinguish between columns needed for grouping versus those used in aggregate calculations