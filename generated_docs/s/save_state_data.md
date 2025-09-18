# save_state_data

## Location
src/backend/access/transam/twophase.c: 1021 - 1048

## Overview
Appends a block of data to the two-phase commit state records data structure, managing memory allocation and alignment for persistent storage.

## Definition
static void save_state_data(const void *data, uint32 len)

## Detailed Description
This static function is responsible for accumulating state data that will be written to disk as part of a prepared transaction's state file. It manages a linked list of data chunks (StateFileChunk structures) that collectively store all the state information needed to recover or complete a prepared transaction.

The function handles memory management by allocating new chunks when the current chunk doesn't have sufficient space. Each data block is padded to MAXALIGN boundaries to ensure proper alignment when the data is later read from disk. The function copies the input data, allowing callers to safely modify their data after the call returns.

## Parameters / Member Variables
- `data`: Pointer to the data block to be saved (const void* - data is copied, not referenced)
- `len`: Length of the data block in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [StateFileChunk](../S/StateFileChunk.md)
- Called from (representative examples):
  - [StartPrepare](../S/StartPrepare.md) (multiple calls for different state components)
  - [RegisterTwoPhaseRecord](../R/RegisterTwoPhaseRecord.md)

## Notes and Other Information
- Each data block is padded to MAXALIGN boundaries, which must be accounted for when reading the file later
- Uses a chunked approach to handle arbitrarily large state data without requiring contiguous memory
- Allocates chunks in increments of at least 512 bytes or the required padded length, whichever is larger
- Maintains running totals of bytes_free, total_len, and num_chunks for efficient management
- The data is copied into the records structure, so callers retain ownership of their original data
- Critical component of the two-phase commit state persistence mechanism