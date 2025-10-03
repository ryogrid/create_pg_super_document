# ginExtractEntries

## Location
[src/backend/access/gin/ginutil.c:483-601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginutil.c#L483-L601)

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
  - [GinState](../G/GinState.md) (structure containing opclass functions and state)
  - `GinNullCategory` (enum for categorizing different types of keys)
  - [FunctionCall3Coll](../F/FunctionCall3Coll.md) (calls the opclass's extractValueFn)
  - [keyEntryData](../k/keyEntryData.md) (structure for temporary key storage during sorting)
  - `cmpEntriesArg` (structure for comparison function arguments)
  - [cmpEntries](../c/cmpEntries.md) (comparison function for sorting keys)
  - `qsort_arg` (system function for sorting with custom comparison)
  - `GIN_CAT_NULL_ITEM`, `GIN_CAT_EMPTY_ITEM`, `GIN_CAT_NULL_KEY`, `GIN_CAT_NORM_KEY` (category constants)
- Called from (representative examples):
  - [ginHeapTupleFastCollect](ginHeapTupleFastCollect.md) (fast insertion path)
  - [ginHeapTupleBulkInsert](ginHeapTupleBulkInsert.md) (bulk insertion operations)
  - [ginHeapTupleInsert](ginHeapTupleInsert.md) (regular tuple insertion)

## Notes and Other Information
- Returns a palloc'd array of Datum values that must be freed by the caller
- Automatically handles duplicate removal to avoid redundant index entries
- Uses qsort for sorting when there are multiple keys (noted as potentially inefficient for small key counts)
- Generates appropriate placeholder entries for NULL and empty items to maintain index consistency
- The returned categories array parallels the entries array and indicates the type of each key
- Supports collation-aware comparison through the GinState's collation information

## Simplified Source

```c
Datum *ginExtractEntries(GinState *ginstate, OffsetNumber attnum,
                        Datum value, bool isNull,
                        int32 *nentries, GinNullCategory **categories) {
    Datum *entries;
    bool *nullFlags;

    // Handle NULL items with placeholder
    if (isNull) {
        *nentries = 1;
        entries = (Datum *) palloc(sizeof(Datum));
        entries[0] = (Datum) 0;
        *categories = (GinNullCategory *) palloc(sizeof(GinNullCategory));
        (*categories)[0] = GIN_CAT_NULL_ITEM;
        return entries;
    }

    // Extract values using opclass function
    nullFlags = NULL;
    entries = (Datum *) DatumGetPointer(FunctionCall3Coll(
        &ginstate->extractValueFn[attnum - 1],
        ginstate->supportCollation[attnum - 1],
        value, PointerGetDatum(nentries), PointerGetDatum(&nullFlags)));

    // Handle empty items with placeholder
    if (entries == NULL || *nentries <= 0) {
        *nentries = 1;
        entries = (Datum *) palloc(sizeof(Datum));
        entries[0] = (Datum) 0;
        *categories = (GinNullCategory *) palloc(sizeof(GinNullCategory));
        (*categories)[0] = GIN_CAT_EMPTY_ITEM;
        return entries;
    }

    // Create null flags if not provided
    if (nullFlags == NULL)
        nullFlags = (bool *) palloc0(*nentries * sizeof(bool));

    // Sort and deduplicate for multiple keys
    if (*nentries > 1) {
        // Sort using cmpEntries and remove duplicates
        keyEntryData *keydata = (keyEntryData *) palloc(*nentries * sizeof(keyEntryData));
        cmpEntriesArg arg;

        // Prepare data for sorting
        for (int i = 0; i < *nentries; i++) {
            keydata[i].datum = entries[i];
            keydata[i].isnull = nullFlags[i];
        }

        arg.cmpDatumFunc = &ginstate->compareFn[attnum - 1];
        arg.collation = ginstate->supportCollation[attnum - 1];
        arg.haveDups = false;

        qsort_arg(keydata, *nentries, sizeof(keyEntryData), cmpEntries, &arg);

        // Remove duplicates if found
        if (arg.haveDups) {
            int j = 1;
            entries[0] = keydata[0].datum;
            nullFlags[0] = keydata[0].isnull;
            for (int i = 1; i < *nentries; i++) {
                if (cmpEntries(&keydata[i - 1], &keydata[i], &arg) != 0) {
                    entries[j] = keydata[i].datum;
                    nullFlags[j] = keydata[i].isnull;
                    j++;
                }
            }
            *nentries = j;
        }
        pfree(keydata);
    }

    // Create category array from null flags
    *categories = (GinNullCategory *) palloc0(*nentries * sizeof(GinNullCategory));
    for (int i = 0; i < *nentries; i++)
        (*categories)[i] = (nullFlags[i] ? GIN_CAT_NULL_KEY : GIN_CAT_NORM_KEY);

    return entries;
}
```