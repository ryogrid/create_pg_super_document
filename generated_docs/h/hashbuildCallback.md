# hashbuildCallback

## Location
src/backend/access/hash/hash.c: 210 - 250

## Overview
Per-tuple callback function used during hash index building to process each heap tuple encountered during the table scan.

## Definition
```c
static void hashbuildCallback(Relation index, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
```

## Detailed Description
The hashbuildCallback function is called for each tuple during the table_index_build_scan operation in hashbuild. It serves as the processing pipeline for converting heap tuples into hash index entries. The function first converts the tuple data into a hash key format, then either spools the tuple for later sorted insertion (if using the sorting optimization) or immediately inserts it into the index.

When spooling is not used, the function creates an IndexTuple and calls _hash_doinsert to place it directly into the appropriate hash bucket. When spooling is used, tuples are stored temporarily and will be sorted by bucket number before bulk insertion to improve I/O locality.

The function maintains a count of processed tuples in the build state for progress reporting and statistics.

## Parameters / Member Variables
- `index`: The hash index being built
- `tid`: ItemPointer to the heap tuple
- `values`: Array of Datum values from the heap tuple
- `isnull`: Array of null indicators for the values
- `tupleIsAlive`: Boolean indicating if the tuple is visible
- `state`: Void pointer to HashBuildState containing build context

## Dependencies
- Functions called/Symbols referenced:
  - _hash_convert_tuple
  - [_h_spool](_h_spool.md)
  - [index_form_tuple](../i/index_form_tuple.md)
  - RelationGetDescr
  - [_hash_doinsert](_hash_doinsert.md)
  - [pfree](../p/pfree.md)
- Called from:
  - [table_index_build_scan](../t/table_index_build_scan.md) (via function pointer in hashbuild)

## Notes and Other Information
- Handles both immediate insertion and spooling strategies based on build state
- Silently skips tuples that cannot be converted to valid hash keys
- Updates the indtuples counter for each successfully processed tuple
- Memory management includes proper cleanup of temporary IndexTuple objects
- The function is static, indicating it's only used within the hash access method implementation