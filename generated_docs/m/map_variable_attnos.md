# map_variable_attnos

## Location
src/backend/rewrite/rewriteManip.c: 1615 - 1665

## Overview
Maps column attribute numbers in Var nodes throughout an expression tree from one relation's schema to another using a provided attribute mapping table.

## Definition


## Detailed Description
This function provides a high-level interface for remapping column attribute numbers in PostgreSQL expression trees. It's commonly used during schema changes, table inheritance operations, partitioning, and other transformations where column positions need to be adjusted between different table definitions.

The function sets up a context structure and delegates the actual tree walking to map_variable_attnos_mutator. It handles both Query nodes and bare expression trees, ensuring that sublevel tracking is properly managed. The function can also convert whole-row variables to different row types when specified.

This utility is essential for maintaining expression correctness when table schemas change or when expressions need to be adapted to reference different but related table structures.

## Parameters / Member Variables
- : The expression tree or Query to process for attribute number mapping
- : The range table entry number whose variables should be remapped
- : The sublevel depth at which to look for the target RTE  
- : Mapping table that specifies how old attribute numbers should be converted to new ones
- : Optional target row type OID for whole-row variable conversion (InvalidOid if not needed)
- : Output parameter set to true if any whole-row variables were encountered

## Dependencies
- Functions called/Symbols referenced:
  - query_or_expression_tree_mutator
  - [map_variable_attnos_mutator](map_variable_attnos_mutator.md)
  - map_variable_attnos_context (struct)
  - [AttrMap](../A/AttrMap.md) (struct)
- Called from (representative examples):
  - [CompareIndexInfo](../C/CompareIndexInfo.md)
  - [map_partition_varattnos](map_partition_varattnos.md)
  - [MergeAttributes](../M/MergeAttributes.md)
  - [ATPrepAlterColumnType](../A/ATPrepAlterColumnType.md)
  - [ExecInitPartitionInfo](../E/ExecInitPartitionInfo.md)
  - [expandTableLikeClause](../e/expandTableLikeClause.md)
  - [generateClonedIndexStmt](../g/generateClonedIndexStmt.md)

## Notes and Other Information
- Widely used throughout PostgreSQL for schema transformation operations
- The found_whole_row parameter is always initialized to false before processing
- Can handle both planned and unplanned expression contexts
- Essential for maintaining expression validity during DDL operations like ALTER TABLE
- Used extensively in partitioning logic to adapt expressions for different partition schemas
- The AttrMap structure defines the old-to-new attribute number mapping rules
- Supports optional row type conversion for whole-row variables, useful in inheritance scenarios