# _h_spooldestroy

## Location
src/backend/access/hash/hashsort.c: 99 - 108

## Overview
Cleans up and deallocates a hash index spool structure and all its associated substructures.

## Definition


## Detailed Description
This function performs cleanup operations for an HSpool structure that was previously created by . It properly terminates the tuplesort state by calling  to release any resources held by the sorting subsystem, then deallocates the HSpool structure itself using . This ensures proper resource management during hash index construction cleanup.

## Parameters / Member Variables
- : Pointer to the HSpool structure to be destroyed

## Dependencies
- Functions called/Symbols referenced:
  - [HSpool](../H/HSpool.md) (structure type)
  - tuplesort_end (terminates tuple sorting state)
  - [pfree](../p/pfree.md) (deallocates memory)
- Called from (representative examples):
  - [hashbuild](hashbuild.md)

## Notes and Other Information
- Must be called after hash index construction is complete to prevent memory leaks
- The function handles cleanup in the correct order: first the tuplesort state, then the main structure
- Should always be paired with a corresponding  call
- Part of the hash index construction cleanup sequence