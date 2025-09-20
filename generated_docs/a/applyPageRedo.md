# applyPageRedo

## Location
[src/backend/access/transam/generic_xlog.c:453-477](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/generic_xlog.c#L453-L477)

## Overview
Applies delta changes to a database page during WAL replay by parsing and applying a series of offset-length-data triplets to reconstruct the modified page.

## Definition

```c
static void
applyPageRedo(Page page, const char *delta, Size deltaSize)
```
## Detailed Description
applyPageRedo is a static function that implements the core logic for applying delta changes during WAL (Write-Ahead Log) replay. It processes a delta buffer containing a series of modifications encoded as offset-length-data triplets. Each triplet specifies where in the page to apply changes (offset), how much data to copy (length), and the actual data bytes to be written.

The function iterates through the delta buffer, parsing each modification record and applying the changes directly to the target page. This delta-based approach is more efficient than storing full page images, especially when only small portions of a page are modified.

## Parameters / Member Variables
- : Target database page where delta changes will be applied
- : Buffer containing encoded delta changes as offset-length-data triplets  
- : Total size of the delta buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - memcpy (copies memory blocks for offset, length, and data extraction/application)
- Called from (representative examples):
  - [generic_redo](../g/generic_redo.md) (applies deltas during WAL replay)

## Notes and Other Information
- This is a static function, only accessible within the generic_xlog.c file
- Delta format consists of repeating [OffsetNumber, length, data] triplets
- Uses OffsetNumber type for page offsets, providing type safety
- No bounds checking is performed - relies on caller to provide valid delta data
- Essential component of PostgreSQL's generic WAL replay mechanism
- Complements computeDelta function which creates the delta records during logging
- Efficient for sparse page modifications where only small portions change