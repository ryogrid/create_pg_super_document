# GetRealCmin

## Location
src/backend/utils/time/combocid.c: 279 - 285

## Overview
GetRealCmin is an internal function that retrieves the original cmin (command ID of insertion) from a combo command ID in PostgreSQL's combo CID system.

## Definition

```c
static CommandId
GetRealCmin(CommandId combocid)
```
## Detailed Description
GetRealCmin is part of PostgreSQL's combo command ID system that was introduced to reduce tuple header size. Since PostgreSQL 8.3, the cmin and cmax fields in tuple headers are overlaid to save space. When both cmin and cmax are needed (typically when a tuple is inserted and deleted within the same transaction), a combo command ID is created and stored instead.

This function takes a combo command ID and returns the original cmin value by performing a simple array lookup into the comboCids array. The function includes an assertion to ensure the provided combo ID is within valid bounds (less than usedComboCids).

## Parameters / Member Variables
- : A combo command ID that serves as an index into the comboCids array to retrieve the original cmin value

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for bounds checking)
  - CommandId (type definition)
- Called from (representative examples):
  - HeapTupleHeaderGetCmin
  - CCID_ARRAY_SIZE (indirectly through array bounds checking)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the combocid.c module
- The function performs a simple array lookup without additional validation beyond the assertion
- The comboCids array is maintained in TopTransactionContext and destroyed at transaction end
- The combo CID system is designed to handle the common case where tuples are not both inserted and deleted in the same transaction, while still supporting that scenario when needed
- The function is critical for translating combo command IDs back to their original cmin values for visibility checking and transaction processing