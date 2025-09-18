# bms_next_member

## Location
src/backend/nodes/bitmapset.c: 1306 - 1366

## Overview
Finds the next member in a bitmap set that is greater than a specified previous bit position, enabling efficient iteration through set members.

## Definition


## Detailed Description
The bms_next_member function is designed to support efficient iteration through the members of a bitmap set. It returns the smallest member that is greater than the specified prevbit value. The function is optimized for sequential access patterns and uses bit manipulation techniques to quickly locate the next set bit.

The function implements a distinctive return value convention: it returns -2 (not -1) when no more members exist. This allows distinguishing between the loop-not-started state (prevbit == -1) and the loop-completed state (return value == -2), which can be useful for complex iteration logic.

The typical usage pattern is:


## Parameters / Member Variables
- : The bitmap set to iterate through (const, not modified, can be NULL)
- : The previous bit position; function returns next member greater than this value (must be >= -1)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md) (input validation)
  - BITNUM (bit position within word calculation)
  - WORDNUM (bit position to word index conversion)
  - BITS_PER_BITMAPWORD (word size constant)
  - bmw_rightmost_one_pos (find rightmost set bit position in word)
  - bitmapword (bitmap word type)
  
- Called from (representative examples):
  - [ExecInitAppend](../E/ExecInitAppend.md)/ExecInitMergeAppend (append node initialization)
  - [choose_next_subplan_locally](../c/choose_next_subplan_locally.md) (subplan selection)
  - [generate_base_implied_equalities](../g/generate_base_implied_equalities.md) (equivalence class processing)
  - [grouping_planner](../g/grouping_planner.md) (grouping and aggregation planning)
  - [get_matching_partitions](../g/get_matching_partitions.md) (partition matching)

## Notes and Other Information
- Returns -2 if no members exist greater than prevbit (not -1)
- Returns -2 immediately if the input bitmap set is NULL
- prevbit must not be less than -1 (behavior unpredictable otherwise)
- Uses efficient bit masking to skip irrelevant bits in the starting word
- Processes subsequent words completely without masking
- Essential for iterating through sparse bitmap sets efficiently
- Extensively used throughout PostgreSQL for set iteration, particularly in:
  - Executor node processing and partition handling
  - [Query](../Q/Query.md) optimization and equivalence class management
  - Statistics collection and dependency analysis
- The distinctive -2 return value enables sophisticated iteration control flow