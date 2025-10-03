# GetRealCmax

## Location
[src/backend/utils/time/combocid.c:286-296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/combocid.c#L286-L296)

## Overview
GetRealCmax is an internal function that retrieves the original cmax (command ID of deletion) from a combo command ID in PostgreSQL's combo CID system.

## Definition

```c
static CommandId
GetRealCmax(CommandId combocid)
```
## Detailed Description
GetRealCmax is part of PostgreSQL's combo command ID system that was introduced to reduce tuple header size. Since PostgreSQL 8.3, the cmin and cmax fields in tuple headers are overlaid to save space. When both cmin and cmax are needed (typically when a tuple is inserted and deleted within the same transaction), a combo command ID is created and stored instead.

This function takes a combo command ID and returns the original cmax value by performing a simple array lookup into the comboCids array. The function includes an assertion to ensure the provided combo ID is within valid bounds (less than usedComboCids). This is the counterpart to GetRealCmin, retrieving the deletion command ID rather than the insertion command ID.

## Parameters / Member Variables
- `combocid`: A combo command ID that serves as an index into the comboCids array to retrieve the original cmax value
## Dependencies
- Functions called/Symbols referenced:
  - Assert (for bounds checking)
  - CommandId (type definition)
- Called from (representative examples):
  - [HeapTupleHeaderGetCmax](../H/HeapTupleHeaderGetCmax.md)
  - CCID_ARRAY_SIZE (indirectly through array bounds checking)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the combocid.c module
- The function performs a simple array lookup without additional validation beyond the assertion
- The comboCids array is maintained in TopTransactionContext and destroyed at transaction end
- The combo CID system is designed to handle the common case where tuples are not both inserted and deleted in the same transaction, while still supporting that scenario when needed
- The function is critical for translating combo command IDs back to their original cmax values for visibility checking and transaction processing
- Works in tandem with GetRealCmin to provide complete cmin/cmax information from combo command IDs

## Simplified Source

```c
static CommandId
GetRealCmax(CommandId combocid)
{
    // Verify combo ID is within valid range
    Assert(combocid < usedComboCids);

    // Return the original cmax from combo CID table
    return comboCids[combocid].cmax;
}
```