# SMgrSortArray

## Location
src/backend/storage/buffer/bufmgr.c: 132 - 136

## Overview
SMgrSortArray is a structure designed for sorting SMgrRelations and ensuring compatibility with RelFileLocator for shared comparator functions in buffer management operations.

## Definition


## Detailed Description
SMgrSortArray is a specialized structure used in PostgreSQL's buffer management system to facilitate sorting operations on SMgrRelation objects. The structure is specifically designed to maintain compatibility between FlushRelationsAllBuffers and DropRelationsAllBuffers functions by ensuring that the RelFileLocator field is positioned as the first member.

This design allows both functions to share the same comparator function, as the memory layout ensures that a pointer to SMgrSortArray can be treated as compatible with a pointer to RelFileLocator for comparison purposes. This is a common C programming pattern that leverages the guaranteed memory layout of structures to enable type punning for optimization.

## Parameters / Member Variables
- : RelFileLocator that identifies the relation's file location; positioned as the first member to ensure pointer compatibility with RelFileLocator for sorting operations
- : SMgrRelation object containing the storage manager relation information

## Dependencies
- Functions called/Symbols referenced:
  - RelFileLocator (type)
  - SMgrRelation (type)

- Called from (representative examples):
  - FlushRelationsAllBuffers (primary usage across multiple locations)

## Notes and Other Information
- The structure is specifically designed for compatibility between FlushRelationsAllBuffers and DropRelationsAllBuffers comparator functions
- The RelFileLocator must be the first member to ensure proper pointer casting compatibility
- This structure enables efficient sorting of storage manager relations during buffer flush operations
- The design follows C structure layout guarantees to allow safe type punning between SMgrSortArray* and RelFileLocator*
- Used internally in buffer management for coordinating flush operations across multiple relations
- The structure is defined in src/backend/storage/buffer/bufmgr.c:132-136
- The memory layout design is critical for the shared comparator function to work correctly with both data types