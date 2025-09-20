# delvacuum_desc

## Location
[src/backend/access/rmgrdesc/nbtdesc.c:196-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/nbtdesc.c#L196-L254)

## Overview
The  function formats detailed information about deleted and updated items in B-tree vacuum and delete operations for WAL record descriptions.

## Definition

```c
structure
	 * that we could use.  Readability seems more important here.)
	 */
	appendStringInfoString(buf, ", updated: [");
```
## Detailed Description
This static helper function provides detailed formatting for B-tree vacuum and delete operations that contain information about deleted and updated items. It parses the block data structure to extract arrays of deleted offset numbers and updated item information, then formats them into a human-readable representation. For deleted items, it displays an array of page offset numbers. For updated items, it shows more complex information including the offset number, number of deleted TIDs (tuple identifiers), and the specific TID positions that were deleted from posting list tuples. The function handles the complex data layout where deleted offsets come first, followed by updated offsets, followed by the detailed update structures.

## Parameters / Member Variables
- : StringInfo buffer where the formatted description will be appended
- : Raw block data containing the deleted and updated item information
- : Number of deleted items in the data
- : Number of updated items in the data

## Dependencies
- Functions called/Symbols referenced:
  - [array_desc](../a/array_desc.md) (for formatting deleted offset arrays)
  - [offset_elem_desc](../o/offset_elem_desc.md) (callback for formatting individual offsets)
  - OffsetNumberIsValid (validation function)
  - appendStringInfo/appendStringInfoString/appendStringInfoChar (string formatting)
  - [xl_btree_update](../x/xl_btree_update.md) (structure type)
  - SizeOfBtreeUpdate (macro for structure size)
- Called from (representative examples):
  - [btree_desc](../b/btree_desc.md) (called twice for VACUUM and DELETE operations)

## Notes and Other Information
- This is a static function only used within nbtdesc.c for B-tree WAL record descriptions
- Handles complex data layout with variable-length structures for update information
- For updated items, shows detailed information about posting list tuple modifications including which specific TID positions were deleted
- Uses assertions to validate offset numbers and ensure updated items have deleted TIDs
- The output format prioritizes readability over literal representation of the physical data structure
- Essential for understanding the specific items affected by B-tree vacuum and delete operations in WAL analysis