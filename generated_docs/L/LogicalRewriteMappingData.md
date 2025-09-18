# LogicalRewriteMappingData

## Location
src/include/access/rewriteheap.h: 35 - 41

## Overview
LogicalRewriteMappingData is a structure that represents the on-disk data format for individual logical rewrite mappings, storing the relationship between old and new tuple locations during table rewrite operations.

## Definition


## Detailed Description
LogicalRewriteMappingData defines the on-disk format for storing logical rewrite mappings that track how tuples are relocated during table rewrite operations. This structure is essential for logical replication and logical decoding, as it maintains the mapping between old and new tuple identifiers (TIDs) when tables are rewritten.

During table rewrite operations (such as VACUUM FULL, CLUSTER, or ALTER TABLE operations that require a full table rebuild), the physical locations of tuples change. Logical replication systems need to track these changes to maintain consistency and properly apply logical changes. The LogicalRewriteMappingData structure provides the necessary mapping information by storing both the file locators (which identify the relation files) and the item pointers (which identify specific tuple locations within those files).

This mapping data is typically written to temporary files during the rewrite operation and later used by logical replication components to translate old tuple references to their new locations.

## Parameters / Member Variables
- : RelFileLocator identifying the original relation file before rewrite
- : RelFileLocator identifying the new relation file after rewrite  
- : ItemPointerData pointing to the original tuple location (block number and item number)
- : ItemPointerData pointing to the new tuple location after rewrite

## Dependencies
- Functions called/Symbols referenced:
  - [RelFileLocator](../R/RelFileLocator.md) (relation file identification)
  - [ItemPointerData](../I/ItemPointerData.md) (tuple location identification)

- Called from (representative examples):
  - [RewriteMappingDataEntry](../R/RewriteMappingDataEntry.md) (internal mapping structure)
  - [logical_heap_rewrite_flush_mappings](../l/logical_heap_rewrite_flush_mappings.md) (writing mappings to disk)
  - [logical_rewrite_log_mapping](../l/logical_rewrite_log_mapping.md) (logging mapping operations)
  - [logical_rewrite_heap_tuple](../l/logical_rewrite_heap_tuple.md) (tuple rewriting with mapping)
  - heap_xlog_logical_rewrite (WAL replay of logical rewrites)
  - [ApplyLogicalMappingFile](../A/ApplyLogicalMappingFile.md) (applying mappings during logical replication)

## Notes and Other Information
- This structure is specifically designed for on-disk storage and has a fixed binary format
- The mapping data is essential for maintaining consistency in logical replication during table rewrites
- Mappings are typically stored in temporary files during rewrite operations and processed by logical replication subsystems
- The structure provides bidirectional mapping capability between old and new tuple locations
- Used extensively in logical replication infrastructure to handle table rewrites transparently
- The data format must remain stable across PostgreSQL versions to ensure compatibility with stored mapping files