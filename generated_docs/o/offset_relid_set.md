# offset_relid_set

## Location
src/backend/rewrite/rewriteManip.c: 533 - 561

## Overview
A static utility function that applies a relation table offset to all members of a Relids (relation identifier set), creating a new set with adjusted relation IDs.

## Definition


## Detailed Description
This function creates a new Relids set by applying a specified offset to each relation identifier in the input set. It efficiently handles the case where no offset is needed (rtoffset == 0) by returning the original set unchanged. For non-zero offsets, it iterates through all members of the input set using the bitmap set utilities, adds the offset to each relation ID, and builds a new set with the adjusted values. This function is essential for maintaining correct relation references when range tables are combined or when relation IDs need to be renumbered during query processing.

## Parameters / Member Variables
- `relids`: The input Relids set containing relation identifiers to be offset
- `rtoffset`: The integer offset value to add to each relation ID in the set

## Dependencies
- Functions called/Symbols referenced:
  - bms_next_member (to iterate through set members)
  - bms_add_member (to add members to the result set)
  - Relids (bitmap set type for relation identifiers)
- Called from (representative examples):
  - OffsetVarNodes_walker (for adjusting variable nulling relations)
  - fix_scan_list
  - set_foreignscan_references
  - set_customscan_references
  - set_append_references
  - set_mergeappend_references

## Notes and Other Information
- This is a static function within setrefs.c, indicating internal use within the optimizer
- Optimizes for the common case where rtoffset is 0 by returning the original set
- Uses PostgreSQL's bitmap set (bms) utilities for efficient set operations
- Essential for maintaining referential integrity when combining range tables
- Used extensively during plan reference adjustment in the optimizer
- Also used in rewrite manipulation when adjusting variable references
- Returns a newly allocated Relids set, so callers are responsible for memory management
- Part of the broader infrastructure for handling relation ID adjustments during query processing