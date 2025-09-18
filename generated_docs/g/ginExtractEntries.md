# ginExtractEntries

## Location
src/backend/access/gin/ginutil.c: 483 - 601

## Overview
Extracts and processes index key values from an indexable item for GIN indexes, handling sorting, deduplication, and special cases for NULL and empty items.

## Definition
```c
Datum *ginExtractEntries(GinState *ginstate, OffsetNumber attnum,
                        Datum value, bool isNull,
                        int32 *nentries, GinNullCategory **categories)
```

## Detailed Description
The `ginExtractEntries` function is a core component of GIN index processing that extracts key values from indexable items and prepares them for storage in the index. It handles three main scenarios: NULL items (generates a NULL placeholder), empty items (generates an EMPTY placeholder), and regular items (calls the opclass's extractValueFn). For regular items with multiple keys, it performs sorting using `qsort_arg` with the `cmpEntries` comparison function and removes duplicates to avoid redundant index entries. The function also manages NULL flags and converts them to GinNullCategory representations for proper categorization of different types of keys.

## Parameters / Member Variables
- `ginstate`: Pointer to GinState structure containing opclass functions and collation information
- `attnum`: Attribute number (1-based) identifying which attribute/column is being processed
- `value`: The input Datum value to extract keys from
- `isNull`: Boolean flag indicating whether the input value is NULL
- `nentries`: Output parameter returning the number of extracted key entries
- `categories`: Output parameter returning an array of GinNullCategory values for each key

## Dependencies
- Functions called/Symbols referenced:
  - `[GinState](../G/GinState.md)` (structure containing opclass functions and state)
  - `GinNullCategory` (enum for categorizing different types of keys)
  - `[FunctionCall3Coll](../F/FunctionCall3Coll.md)` (calls the opclass's extractValueFn)
  - `[keyEntryData](../k/keyEntryData.md)` (structure for temporary key storage during sorting)
  - `cmpEntriesArg` (structure for comparison function arguments)
  - `[cmpEntries](../c/cmpEntries.md)` (comparison function for sorting keys)
  - `qsort_arg` (system function for sorting with custom comparison)
  - `GIN_CAT_NULL_ITEM`, `GIN_CAT_EMPTY_ITEM`, `GIN_CAT_NULL_KEY`, `GIN_CAT_NORM_KEY` (category constants)
- Called from (representative examples):
  - `[ginHeapTupleFastCollect](ginHeapTupleFastCollect.md)` (fast insertion path)
  - `[ginHeapTupleBulkInsert](ginHeapTupleBulkInsert.md)` (bulk insertion operations)
  - `[ginHeapTupleInsert](ginHeapTupleInsert.md)` (regular tuple insertion)

## Notes and Other Information
- Returns a palloc'd array of Datum values that must be freed by the caller
- Automatically handles duplicate removal to avoid redundant index entries
- Uses qsort for sorting when there are multiple keys (noted as potentially inefficient for small key counts)
- Generates appropriate placeholder entries for NULL and empty items to maintain index consistency
- The returned categories array parallels the entries array and indicates the type of each key
- Supports collation-aware comparison through the GinState's collation information