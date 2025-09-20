# BTScanOpaqueData

## Location
[src/include/access/nbtree.h:1040-1079](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L1040-L1079)

## Overview
BTScanOpaqueData is the comprehensive btree-private state structure that manages all aspects of an index scan, including preprocessed keys, array support, position tracking, and tuple storage.

## Definition

```c
typedef struct BTScanOpaqueData
{
	/* these fields are set by _bt_preprocess_keys(): */
	bool		qual_ok;		/* false if qual can never be satisfied */
	int			numberOfKeys;	/* number of preprocessed scan keys */
	ScanKey		keyData;		/* array of preprocessed scan keys */

	/* workspace for SK_SEARCHARRAY support */
	int			numArrayKeys;	/* number of equality-type array keys */
	bool		needPrimScan;	/* New prim scan to continue in current dir? */
	bool		scanBehind;		/* Last array advancement matched -inf attr? */
	BTArrayKeyInfo *arrayKeys;	/* info about each equality-type array key */
	FmgrInfo   *orderProcs;		/* ORDER procs for required equality keys */
	MemoryContext arrayContext; /* scan-lifespan context for array data */

	/* info about killed items if any (killedItems is NULL if never used) */
	int		   *killedItems;	/* currPos.items indexes of killed items */
	int			numKilled;		/* number of currently stored items */

	/*
	 * If we are doing an index-only scan, these are the tuple storage
	 * workspaces for the currPos and markPos respectively.  Each is of size
	 * BLCKSZ, so it can hold as much as a full page's worth of tuples.
	 */
	char	   *currTuples;		/* tuple storage for currPos */
	char	   *markTuples;		/* tuple storage for markPos */

	/*
	 * If the marked position is on the same page as current position, we
	 * don't use markPos, but just keep the marked itemIndex in markItemIndex
	 * (all the rest of currPos is valid for the mark position). Hence, to
	 * determine if there is a mark, first look at markItemIndex, then at
	 * markPos.
	 */
	int			markItemIndex;	/* itemIndex, or -1 if not valid */

	/* keep these last in struct for efficiency */
	BTScanPosData currPos;		/* current position data */
	BTScanPosData markPos;		/* marked position, if any */
} BTScanOpaqueData;
```
## Detailed Description
This structure serves as the central control hub for B-tree index scans, containing preprocessed scan keys, array scan support, position management, and tuple storage. It implements the page-at-a-time scanning approach where pages are pinned and read-locked, matching items are identified and saved, then the read-lock is released while items are returned to the caller. This minimizes lock/unlock traffic while maintaining proper VACUUM synchronization.

## Parameters / Member Variables
- `qual_ok`: Boolean flag indicating if the qualification can ever be satisfied
- `numberOfKeys`: Integer count of preprocessed scan keys
- `keyData`: ScanKey array containing preprocessed scan keys
- `numArrayKeys`: Integer count of equality-type array keys for SK_SEARCHARRAY support
- `needPrimScan`: Boolean indicating if a new primary scan is needed to continue in current direction
- `scanBehind`: Boolean flag indicating if last array advancement matched negative infinity attribute
- `*arrayKeys`: Pointer to BTArrayKeyInfo array with information about each equality-type array key
- `*orderProcs`: Pointer to FmgrInfo array containing ORDER procedures for required equality keys
- `arrayContext`: MemoryContext providing scan-lifespan context for array data
- `*killedItems`: Pointer to integer array of currPos.items indexes for killed items (NULL if unused)
- `numKilled`: Integer count of currently stored killed items
- `*currTuples`: Character pointer to tuple storage workspace for current position (BLCKSZ size)
- `*markTuples`: Character pointer to tuple storage workspace for marked position (BLCKSZ size)
- `markItemIndex`: Integer itemIndex for marked position (-1 if not valid)
- `currPos`: BTScanPosData structure containing current position data
- `markPos`: BTScanPosData structure containing marked position data if any
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