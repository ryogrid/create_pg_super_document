# _bt_preprocess_array_keys_final

## Location
src/backend/access/nbtree/nbtutils.c: 551 - 711

## Overview
Finalizes array scan key preprocessing by fixing up scan key references, setting up ORDER procedures, and converting single-element arrays into equivalent non-array equality scan keys.

## Definition


## Detailed Description
This function performs the final phase of array scan key preprocessing after the main preprocessing steps are complete. It handles several critical finalization tasks:

1. **Reference Remapping**: Translates scan key references in BTArrayKeyInfo from input scan key offsets (scan->keyData[]) to output scan key offsets (so->keyData[]) using the provided keyDataMap.

2. **ORDER Procedure Setup**: 
   - Repositions existing ORDER procedures for array keys to match their new positions in so->keyData[]
   - Sets up ORDER procedures for non-array equality scan keys that survived preprocessing
   - Skips ORDER procedure setup for IS NULL scan keys and non-required scan keys

3. **Single-Element Array Optimization**: Converts array scan keys with exactly one element into equivalent non-array equality scan keys, which provides a runtime performance benefit since non-array equality operations are slightly faster than array operations.

4. **Parallel Scan Validation**: For parallel index scans, validates that the number of remaining array keys doesn't exceed INDEX_MAX_KEYS to prevent issues with shared memory structures protected by spinlocks.

The function operates in-place and can completely eliminate arrays from a scan if all arrays are reduced to single elements.

## Parameters / Member Variables
- : The index scan descriptor containing the scan keys and array information
- : Array mapping input scan key indices to output scan key indices

## Dependencies
- Functions called/Symbols referenced:
  - BTScanOpaque
  - BTArrayKeyInfo
  - _bt_setup_array_cmp
  - memmove
  - ereport
  - SK_SEARCHARRAY, SK_SEARCHNULL, SK_BT_REQFWD
  - InvalidStrategy
  - INDEX_MAX_KEYS
  - PG_USED_FOR_ASSERTS_ONLY

- Called from (representative examples):
  - _bt_preprocess_keys (final step in scan key preprocessing)

## Notes and Other Information
- Returns early if so->numArrayKeys is 0, indicating no array keys need finalization
- The function assumes that equality strategy scan keys appear in original input order within each group of entries for the same index attribute
- Single-element array transformation decrements so->numArrayKeys and may leave the scan with no arrays at all
- When arrays are removed due to single-element optimization, remaining arrays are shifted forward in the BTArrayKeyInfo array
- For parallel scans, the function enforces a limit on the number of array keys to prevent excessive shared memory usage
- The optimization of converting single-element arrays to non-array keys is purely for performance and not required for correctness