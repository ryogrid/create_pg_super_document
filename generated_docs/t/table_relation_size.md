# table_relation_size

## Location
src/include/access/tableam.h: 1869 - 1877

## Overview
Returns the size of a table relation in bytes, either for a specific fork or the overall relation size.

## Definition


## Detailed Description
This function provides a table access method (tableam) interface for determining the size of a relation. It serves as a wrapper that calls the appropriate relation_size function from the table access method's function pointer table (rd_tableam). The function can return either the size of a specific fork when a valid ForkNumber is provided, or the overall relation size when InvalidForkNumber is passed.

The implementation delegates to the underlying table access method, allowing different storage engines to provide their own size calculation logic. This abstraction is important because different access methods may organize their storage differently, and the overall size might not simply be the sum of individual fork sizes.

## Parameters / Member Variables
- : A Relation pointer representing the table relation whose size is being queried
- : Specifies which fork's size to return. If InvalidForkNumber is passed, returns the overall relation size

## Dependencies
- Functions called/Symbols referenced:
  - rel->rd_tableam->relation_size (table access method function pointer)
- Called from (representative examples):
  - [RelationGetNumberOfBlocksInFork](../R/RelationGetNumberOfBlocksInFork.md) (in src/backend/storage/buffer/bufmgr.c:3924)

## Notes and Other Information
- This is an inline function defined in the tableam header file, providing efficient access to the underlying table access method
- The function is part of the table access method abstraction layer, allowing PostgreSQL to support different storage engines
- Different access methods may calculate size differently, and the overall size may not equal the sum of individual fork sizes
- Located in src/include/access/tableam.h:1869-1877