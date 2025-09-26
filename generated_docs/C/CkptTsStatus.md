# CkptTsStatus

## Location
src/backend/storage/buffer/bufmgr.c: 101 - 123

## Overview
CkptTsStatus is a structure used internally by BufferSync to track the checkpoint progress and status for individual tablespaces during buffer synchronization operations.

## Definition

```c
typedef struct CkptTsStatus
{
	/* oid of the tablespace */
	Oid			tsId;

	/*
	 * Checkpoint progress for this tablespace. To make progress comparable
	 * between tablespaces the progress is, for each tablespace, measured as a
	 * number between 0 and the total number of to-be-checkpointed pages. Each
	 * page checkpointed in this tablespace increments this space's progress
	 * by progress_slice.
	 */
	float8		progress;
	float8		progress_slice;

	/* number of to-be checkpointed pages in this tablespace */
	int			num_to_scan;
	/* already processed pages in this tablespace */
	int			num_scanned;

	/* current offset in CkptBufferIds for this tablespace */
	int			index;
} CkptTsStatus;
```
## Detailed Description
CkptTsStatus is a crucial data structure in PostgreSQL's checkpoint mechanism that maintains per-tablespace state during buffer synchronization. This structure allows the checkpoint process to track progress across multiple tablespaces in a coordinated manner, ensuring fair distribution of I/O operations and providing accurate progress reporting.

The structure is designed to enable smooth checkpoint progress by managing how pages from different tablespaces are written to disk. It implements a progress tracking system that normalizes progress across tablespaces regardless of their individual sizes, allowing for more predictable checkpoint behavior.

## Parameters / Member Variables
- : The OID (Object Identifier) of the tablespace being checkpointed
- : Current checkpoint progress for this tablespace, normalized as a value between 0 and the total pages to be checkpointed
- : The increment value added to progress for each page checkpointed in this tablespace
- : Total number of pages that need to be checkpointed in this tablespace
- : Number of pages already processed/checkpointed in this tablespace
- : Current position/offset in the CkptBufferIds array for this tablespace's buffers

## Dependencies
- Functions called/Symbols referenced:
  - Oid (type)
  - float8 (type)
  - int (type)

- Called from (representative examples):
  - BufferSync (primary usage across multiple locations)
  - ts_ckpt_progress_comparator

## Notes and Other Information
- This structure is used internally by the BufferSync function to coordinate checkpoint operations across multiple tablespaces
- The progress tracking mechanism helps ensure fair I/O distribution during checkpoints, preventing any single tablespace from dominating the checkpoint process
- The progress_slice calculation allows checkpoints to proceed smoothly regardless of the relative sizes of different tablespaces
- The structure is defined in src/backend/storage/buffer/bufmgr.c:101-123
- Used in conjunction with checkpoint buffer management to optimize disk I/O patterns during checkpoint operations
- The ts_ckpt_progress_comparator function uses this structure for sorting tablespaces by checkpoint progress