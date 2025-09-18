# BTScanKeyPreproc

## Location
src/backend/access/nbtree/nbtutils.c: 42 - 47

## Overview
BTScanKeyPreproc is a preprocessing structure used during B-tree scan key analysis to track scan keys by strategy type while maintaining references to their original positions and array indices.

## Definition


## Detailed Description
This structure serves as a temporary preprocessing container in _bt_preprocess_keys() to organize scan keys by strategy type during the key optimization phase. It maintains mappings between the transformed scan key, its original index position, and its associated array index (for array scan keys). The structure is used in an array indexed by strategy numbers to help eliminate redundant keys, detect contradictory conditions, and determine which keys are required for continued scanning.

## Parameters / Member Variables
- : Pointer to the ScanKey structure containing the actual scan key data, operator strategy, and value information
- : Integer index indicating the original position of this scan key in the input scan->keyData[] array before preprocessing
- : Integer index tracking the position in array scan keys when SK_SEARCHARRAY flags are present, used for array key processing

## Dependencies
- Functions called/Symbols referenced:
  - ScanKey (struct type)
- Called from (representative examples):
  - [_bt_preprocess_keys](../b/_bt_preprocess_keys.md)

## Notes and Other Information
- Used as a local array  in _bt_preprocess_keys() where each element corresponds to a specific strategy type
- The structure enables tracking of the "best" (most restrictive) scan key for each strategy while preserving original positioning information
- Essential for array scan key processing where maintaining the relationship between scan keys and their array indices is critical
- The preprocessing phase can eliminate redundant keys, detect contradictions, and mark required keys for forward/backward scan continuation
- Only used internally within the scan key preprocessing logic and not exposed outside nbtutils.c