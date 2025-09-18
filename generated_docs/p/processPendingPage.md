# processPendingPage

## Location
src/backend/access/gin/ginfast.c: 709 - 779

## Overview
Collects data from a pending-list page in preparation for insertion into the main GIN index by processing all tuples and accumulating their keys.

## Definition
```c
static void processPendingPage(BuildAccumulator *accum, KeyArray *ka, Page page, OffsetNumber startoff)
```

## Detailed Description
This function processes all tuples starting from a specified offset on a pending-list page, extracting their keys and categories for insertion into the main GIN index. It groups keys by heap tuple ID and attribute number, calling ginInsertBAEntries whenever it encounters a boundary (change in heap TID or attribute). The function efficiently handles the transition from the fast insertion pending list to the main index structure by collecting and organizing the data appropriately.

## Parameters / Member Variables
- `accum`: Pointer to BuildAccumulator structure for collecting index entries
- `ka`: Pointer to KeyArray workspace for temporarily storing keys (reset on each call)
- `page`: The pending-list page to process
- `startoff`: The offset number to start processing from

## Dependencies
- Functions called/Symbols referenced:
  - PageGetMaxOffsetNumber (get maximum offset on page)
  - PageGetItem, PageGetItemId (page item access functions)
  - gintuple_get_attrnum (extract attribute number from tuple)
  - gintuple_get_key (extract key and category from tuple)
  - ginInsertBAEntries (insert accumulated entries into build accumulator)
  - addDatum (add datum to KeyArray)
  - ItemPointerSetInvalid, ItemPointerIsValid, ItemPointerEquals (heap pointer utilities)
  - OffsetNumberNext (offset number iteration)
- Called from (representative examples):
  - ginInsertCleanup (at src/backend/access/gin/ginfast.c:893)
  - ginInsertCleanup (at src/backend/access/gin/ginfast.c:954)

## Notes and Other Information
- This is a static function, accessible only within the ginfast.c file
- The KeyArray (ka) serves as workspace and doesn't carry state across calls
- Groups keys by heap tuple ID and attribute number for efficient batch insertion
- Processes tuples sequentially from startoff to the maximum offset on the page
- Calls ginInsertBAEntries at boundaries to handle batched insertions
- Ensures all remaining keys are dumped out after processing all tuples
- Part of the GIN index pending list cleanup mechanism
- Handles the transition from fast insertion to main index structure