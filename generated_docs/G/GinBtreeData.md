# GinBtreeData

## Location
src/include/access/gin_private.h: 150 - 180

## Overview
GinBtreeData is a comprehensive structure that provides the operational interface for GIN B-tree manipulation through function pointers and maintains context information for both entry trees and data trees.

## Definition


## Detailed Description
GinBtreeData implements a polymorphic interface for GIN B-tree operations through function pointers, allowing both entry trees and data trees to share common navigation and modification algorithms while providing type-specific implementations. The structure combines operational methods with context information, supporting both search operations and tree modifications. It distinguishes between entry trees (which store keys) and data trees (posting trees, which store item pointers) through the isData flag and provides appropriate search keys for each type.

## Parameters / Member Variables
- : Function pointer to locate appropriate child page during tree descent
- : Function pointer to find the leftmost child page at a given level
- : Function pointer to determine if right-link following is needed
- : Function pointer to locate specific items within a page
- : Function pointer to find child page pointer within an internal page
- : Function pointer to initiate page insertion/split operations
- : Function pointer to execute page modification operations
- : Function pointer to prepare downlink data for parent updates
- : Function pointer to populate a new root page after splits
- : Boolean flag indicating whether this is a data tree (true) or entry tree (false)
- : Relation object representing the GIN index
- : Block number of the tree's root page
- : Pointer to GinState structure (not used during data scans)
- : Boolean indicating whether a full scan is being performed
- : Boolean indicating whether this is during index build
- : Attribute number for entry tree searches
- : Search key value for entry tree operations
- : Null category for the entry key
- : Item pointer data for data tree searches

## Dependencies
- Functions called/Symbols referenced:
  - [GinBtree](GinBtree.md) (parameter type for function pointers)
  - [GinBtreeStack](GinBtreeStack.md) (parameter type for function pointers)
  - [GinPlaceToPageRC](GinPlaceToPageRC.md) (return type for beginPlaceToPage)
  - [GinState](GinState.md) (for ginstate member)
  - GinNullCategory (for entryCategory member)
- Called from (representative examples):
  - [ginPrepareDataScan](../g/ginPrepareDataScan.md)
  - [ginPrepareEntryScan](../g/ginPrepareEntryScan.md)
  - [ginInsertItemPointers](../g/ginInsertItemPointers.md)
  - [moveRightIfItNeeded](../m/moveRightIfItNeeded.md)
  - [scanPostingTree](../s/scanPostingTree.md)

## Notes and Other Information
- Located in src/include/access/gin_private.h:150-180
- Implements polymorphic behavior for different GIN tree types
- Function pointers are set differently for entry trees vs data trees
- Essential for abstracting differences between tree types while sharing common algorithms
- The ginstate pointer is not valid during data scans, as noted in the comment
- Supports both build-time and runtime operations through the isBuild flag