# merge_map_updates

## Location
src/backend/utils/cache/relmapper.c: 416 - 437

## Overview
A static helper function that applies multiple relation mapping updates in bulk by merging all mappings from a pending-update map into a target map.

## Definition


## Detailed Description
The  function serves as a bulk operation wrapper around . It iterates through all mappings in the  RelMapFile and applies each one to the target  using the  function. This provides an efficient way to process multiple mapping changes as a single atomic operation, which is essential for maintaining consistency in PostgreSQL's relation mapping system during transactions and command completion.

## Parameters / Member Variables
- : Pointer to the target RelMapFile structure where the updates will be applied
- : Pointer to a const RelMapFile containing all the pending mapping updates to be merged
- : Boolean flag passed through to , indicating whether new mappings can be created (true) or if only existing mappings should be updated (false)

## Dependencies
- Functions called/Symbols referenced:
  - RelMapFile (structure)
  - apply_map_update (function)
- Called from (representative examples):
  - AtCCI_RelationMap
  - perform_relmap_update

## Notes and Other Information
- This is a static function, only accessible within the relmapper.c file
- Provides a bulk interface to , making it more efficient than calling  individually for each mapping
- Used during transaction completion and command completion to apply accumulated mapping changes
- The function maintains the same error handling semantics as  through the  parameter
- Part of PostgreSQL's transactional relation mapping system that ensures mapping changes are applied atomically