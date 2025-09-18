# CloseTableList

## Location
src/backend/commands/publicationcmds.c: 1699 - 1718

## Overview
Closes all relations in a PublicationRelInfo list and performs cleanup of associated memory structures.

## Definition


## Detailed Description
CloseTableList is a static utility function that properly closes database relations that were previously opened by OpenTableList. The function performs a clean shutdown sequence:

1. **Relation Closure**: Iterates through each PublicationRelInfo structure in the provided list and closes the associated database relation using table_close() with NoLock (since the lock was already acquired during opening)
2. **Memory Cleanup**: Calls list_free_deep() to recursively free all memory associated with the list and its contained PublicationRelInfo structures

This function serves as the complementary cleanup operation to OpenTableList, ensuring proper resource management and lock release.

## Parameters / Member Variables
- : List of PublicationRelInfo structures containing opened relations that need to be closed and cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - table_close (closes the database relation)
  - [list_free_deep](../l/list_free_deep.md) (frees list and contained structures)
  - [PublicationRelInfo](../P/PublicationRelInfo.md) (structure type for relation information)
- Called from (representative examples):
  - [CreatePublication](CreatePublication.md) (src/backend/commands/publicationcmds.c:840)
  - [AlterPublicationTables](../A/AlterPublicationTables.md) (src/backend/commands/publicationcmds.c:1237)
  - [AlterPublicationTables](../A/AlterPublicationTables.md) (src/backend/commands/publicationcmds.c:1240)

## Notes and Other Information
- Uses NoLock parameter for table_close() since locks were acquired during the opening phase and are released automatically
- Essential for proper cleanup in publication operations to prevent resource leaks
- Always paired with OpenTableList in publication management workflows
- The list_free_deep() call ensures both the list structure and PublicationRelInfo contents are properly deallocated