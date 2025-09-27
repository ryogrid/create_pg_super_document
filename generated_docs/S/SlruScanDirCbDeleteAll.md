# SlruScanDirCbDeleteAll

## Location
[src/backend/access/transam/slru.c:1741-1754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L1741-L1754)

## Overview
A callback function used by SlruScanDirectory to delete all SLRU segments unconditionally.

## Definition
```c
bool SlruScanDirCbDeleteAll(SlruCtl ctl, char *filename, int64 segpage, void *data)
```

## Detailed Description
SlruScanDirCbDeleteAll is a straightforward callback function that provides unconditional deletion of all SLRU segments encountered during directory scanning. Unlike other SLRU callbacks that apply conditional logic based on cutoff criteria, this callback deletes every segment it encounters without any conditions. It is typically used during cleanup operations where all SLRU data needs to be removed, such as during deactivation of certain subsystems or complete resets. The function converts the segment page number to a segment number and calls SlruInternalDeleteSegment to perform the actual file deletion.

## Parameters / Member Variables
- `ctl`: SlruCtl structure containing SLRU control information and configuration
- `filename`: Name of the SLRU segment file being examined (unused in this callback)
- `segpage`: Page number representing the first page of the segment being examined
- `data`: Void pointer for additional data (unused in this callback)

## Dependencies
- Functions called/Symbols referenced:
  - [SlruInternalDeleteSegment](SlruInternalDeleteSegment.md)
  - SLRU_PAGES_PER_SEGMENT
- Called from (representative examples):
  - [DeactivateCommitTs](../D/DeactivateCommitTs.md)
  - [AsyncShmemInit](../A/AsyncShmemInit.md)
  - [test_slru_scan_cb](../t/test_slru_scan_cb.md)

## Notes and Other Information
- Always returns false to ensure all segments in the directory are processed and deleted
- Used in scenarios requiring complete cleanup of SLRU data
- Part of subsystem deactivation procedures, particularly for commit timestamp tracking
- Also used in test modules for cleanup operations

## Simplified Source

```c
// Simplified version of SlruScanDirCbDeleteAll
bool SlruScanDirCbDeleteAll(SlruCtl ctl, char *filename, int64 segpage, void *data) {
    // Core logic: Delete the segment and continue scanning
    SlruInternalDeleteSegment(ctl, segpage / SLRU_PAGES_PER_SEGMENT);

    return false; // Continue scanning all segments
}
```

Key simplifications made:
- Focused on the single core operation: delete segment
- Removed unused parameter comments
- Emphasized the "continue scanning" return value behavior
- Consolidated the logic into its essential function: unconditional deletion