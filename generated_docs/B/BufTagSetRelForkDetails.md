# BufTagSetRelForkDetails

## Location
src/include/storage/buf_internals.h: 115 - 122

## Overview
Sets the relation number and fork number fields in a BufferTag structure for buffer identification.

## Definition
static inline void
BufTagSetRelForkDetails(BufferTag *tag, RelFileNumber relnumber, ForkNumber forknum)

## Detailed Description
BufTagSetRelForkDetails is an inline utility function that simultaneously sets both the relation number and fork number fields of a BufferTag structure. This function provides a convenient way to initialize or update the relation-specific and fork-specific components of a buffer tag in a single operation. It is commonly used during buffer tag initialization and when creating new buffer tags for specific relation forks.

## Parameters / Member Variables
- tag: Pointer to a BufferTag structure to be modified
- relnumber: The RelFileNumber identifying the specific relation
- forknum: The ForkNumber identifying which fork of the relation (main, FSM, VM, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - BufferTag (structure type)
  - RelFileNumber (type)
  - ForkNumber (type)
- Called from (representative examples):
  - ClearBufferTag
  - InitBufferTag

## Notes and Other Information
- This is an inline function defined in buf_internals.h for performance efficiency
- Provides a clean interface for setting multiple related fields atomically
- Part of the buffer tag utility functions that abstract BufferTag field manipulation
- Used during buffer tag initialization and modification operations
- Does not set other BufferTag fields like RelFileLocator or block number