# BTScanOpaqueData

## Location
src/include/access/nbtree.h: 1040 - 1079

## Overview
BTScanOpaqueData is the comprehensive btree-private state structure that manages all aspects of an index scan, including preprocessed keys, array support, position tracking, and tuple storage.

## Definition


## Detailed Description
This structure serves as the central control hub for B-tree index scans, containing preprocessed scan keys, array scan support, position management, and tuple storage. It implements the page-at-a-time scanning approach where pages are pinned and read-locked, matching items are identified and saved, then the read-lock is released while items are returned to the caller. This minimizes lock/unlock traffic while maintaining proper VACUUM synchronization.

## Parameters / Member Variables
- : Boolean flag indicating if the qualification can ever be satisfied
- : Integer count of preprocessed scan keys
- : ScanKey array containing preprocessed scan keys
- : Integer count of equality-type array keys for SK_SEARCHARRAY support
- : Boolean indicating if a new primary scan is needed to continue in current direction
- : Boolean flag indicating if last array advancement matched negative infinity attribute
- : Pointer to BTArrayKeyInfo array with information about each equality-type array key
- : Pointer to FmgrInfo array containing ORDER procedures for required equality keys
- : MemoryContext providing scan-lifespan context for array data
- : Pointer to integer array of currPos.items indexes for killed items (NULL if unused)
- : Integer count of currently stored killed items
- : Character pointer to tuple storage workspace for current position (BLCKSZ size)
- : Character pointer to tuple storage workspace for marked position (BLCKSZ size)
- : Integer itemIndex for marked position (-1 if not valid)
- : BTScanPosData structure containing current position data
- : BTScanPosData structure containing marked position data if any

## Dependencies
- Functions called/Symbols referenced:
  - ScanKey
  - [BTArrayKeyInfo](BTArrayKeyInfo.md)
  - [FmgrInfo](../F/FmgrInfo.md)
  - [MemoryContext](../M/MemoryContext.md)
  - [BTScanPosData](BTScanPosData.md)
- Called from (representative examples):
  - [btbeginscan](../b/btbeginscan.md)
  - BTScanOpaque

## Notes and Other Information
- Core structure for B-tree access method index scanning operations
- Supports both regular and index-only scans with appropriate tuple storage
- Manages complex array-based scan keys for IN clause and array operations
- Implements efficient mark/restore functionality with position optimization
- Essential for VACUUM synchronization through proper buffer management
- The currPos and markPos fields are kept last for memory layout efficiency
- Killed items tracking enables efficient cleanup of logically deleted entries