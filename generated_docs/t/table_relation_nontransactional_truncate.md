# table_relation_nontransactional_truncate

## Location
src/include/access/tableam.h: 1640 - 1651

## Overview
A table access method wrapper function that removes all contents from a relation in a non-transactional manner, typically used for truncating storage created within the current transaction.

## Definition


## Detailed Description
This function provides a high-level interface for performing non-transactional truncation of table contents. Unlike regular TRUNCATE operations, this function does not need to support rollback capabilities, making it suitable for operations on temporary storage or relations created within the current transaction where rollback semantics are not required.

The function delegates to the table access method's specific implementation of non-transactional truncation, allowing different storage engines to optimize the truncation process when transactional guarantees are not needed.

## Parameters / Member Variables
- : The relation whose contents should be truncated

## Dependencies
- Functions called/Symbols referenced:
  - rel->rd_tableam->relation_nontransactional_truncate (table access method implementation)
- Called from (representative examples):
  - heap_truncate_one_rel (during heap table truncation operations)

## Notes and Other Information
- This is a non-transactional operation - changes cannot be rolled back
- Primarily used for relations created within the current transaction
- The function is an inline wrapper that delegates to the table access method implementation
- More efficient than transactional truncate since it doesn't need to maintain undo information
- Should not be used on relations that require rollback capability