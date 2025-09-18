# adjust_partition_colnos_using_map

## Location
src/backend/executor/execPartition.c: 1716 - 1800

## Overview
Adjusts partition column numbers using a caller-supplied attribute map instead of assuming to map from the root result relation.

## Definition


## Detailed Description
This function remaps a list of partition column numbers using a provided attribute map (AttrMap). It is a more flexible version of adjust_partition_colnos that allows the caller to specify the mapping instead of using a default root relation mapping. The function validates each attribute number in the input list and translates it using the provided map. It performs bounds checking to ensure the attribute numbers are valid and exist in the map.

## Parameters / Member Variables
- : List of partition column numbers (AttrNumber values) to be remapped
- : Attribute map structure containing the mapping from parent to child attribute numbers

## Dependencies
- Functions called/Symbols referenced:
  - lfirst_int
  - lappend_int
  - [AttrMap](../A/AttrMap.md)
- Called from (representative examples):
  - [ExecInitPartitionInfo](../E/ExecInitPartitionInfo.md)
  - [adjust_partition_colnos](adjust_partition_colnos.md)

## Notes and Other Information
- This is a static function, only accessible within execPartition.c
- Must not be called if no adjustment is required (as noted in comments)
- Performs error checking to ensure attribute numbers are within valid bounds
- Returns a new list with remapped column numbers, does not modify the original list
- Used in partition pruning operations to handle attribute number mapping between parent and child relations