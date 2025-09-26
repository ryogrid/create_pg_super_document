# table_relation_copy_data

## Location
[src/include/access/tableam.h:1652-1678](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1652-L1678)

## Overview
A table access method wrapper function that copies data from a relation to a new file locator, primarily used for low-level operations like changing a relation's tablespace.

## Definition

```c
static inline void
table_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
```
## Detailed Description
This function provides a high-level interface for copying all data from an existing relation to a new storage location specified by a RelFileLocator. The function is designed for low-level administrative operations that require moving relation data to different storage locations, such as tablespace changes.

The new storage location may not have any associated storage files before this function is called - the function is responsible for creating the necessary storage structure and copying all data from the source relation. The function delegates to the table access method's specific implementation to handle the storage-specific details of the copy operation.

## Parameters / Member Variables
- : The source relation whose data should be copied
- : Pointer to the RelFileLocator specifying the destination storage location

## Dependencies
- Functions called/Symbols referenced:
  - rel->rd_tableam->relation_copy_data (table access method implementation)
- Called from (representative examples):
  - [ATExecSetTableSpace](../A/ATExecSetTableSpace.md) (during ALTER TABLE ... SET TABLESPACE operations)

## Notes and Other Information
- This is a low-level operation used primarily for administrative tasks
- The destination storage location may not exist before the function is called
- The function handles the complete copying of relation data, including all storage files
- Used primarily for tablespace movement operations
- The function is an inline wrapper that delegates to the table access method implementation
- Performance can be significant for large relations as all data must be copied