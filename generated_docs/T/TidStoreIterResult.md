# TidStoreIterResult

## Location
src/include/access/tidstore.h: 24 - 30

## Overview
TidStoreIterResult is a result structure used by the TidStore iterator to return tuple identifiers (TIDs) for a single block during iteration over a TidStore.

## Definition


## Detailed Description
TidStoreIterResult serves as the output container for the TidStoreIterateNext() function, which iterates over a TidStore and returns TIDs organized by block. This structure efficiently packages all tuple identifiers belonging to a single database block, allowing callers to process TIDs in a block-oriented manner. The structure is designed to support efficient iteration over large collections of tuple identifiers while maintaining the natural clustering of TIDs by their block numbers.

The offsets array contains the actual offset numbers within the block, ordered in ascending order to facilitate efficient processing. The max_offset field provides optimization hints for operations that need to understand the range of offsets present.

## Parameters / Member Variables
- : The block number for which this result contains tuple identifiers
- : The maximum offset number in the offsets array, used for optimization purposes
- : The number of valid entries in the offsets array
- : Pointer to an array of OffsetNumber values representing tuple positions within the block

## Dependencies
- Functions called/Symbols referenced: 
  - Uses PostgreSQL built-in types: BlockNumber, OffsetNumber
- Called from (representative examples):
  - TidStoreIterateNext
  - lazy_vacuum_heap_rel (in vacuum operations)
  - check_set_block_offsets (in test modules)

## Notes and Other Information
- The structure is returned by reference from TidStoreIterateNext(), so callers should not modify or free the structure or its offsets array
- The offsets array is maintained in sorted order to facilitate efficient processing
- This structure is used internally by PostgreSQL's TidStore system for managing collections of tuple identifiers, particularly in vacuum operations
- The structure is designed to be lightweight and efficient for high-frequency iteration operations
- Memory management for the offsets array is handled internally by the TidStore iterator