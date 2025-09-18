# mxtruncinfo

## Location
[src/backend/access/transam/multixact.c:3007-3010](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L3007-L3010)

## Overview
mxtruncinfo is a simple data structure used during MultiXact SLRU truncation operations to track the earliest existing page number when scanning directories.

## Definition
```c
typedef struct mxtruncinfo
{
    int64 earliestExistingPage;
} mxtruncinfo;
```

## Detailed Description
mxtruncinfo serves as a minimal data passing structure used specifically during MultiXact SLRU (Simple Least Recently Used) truncation operations. It acts as a parameter container for directory scanning callbacks that need to determine the earliest existing page in the multixact storage system.

The structure is designed for use with SLRU directory scanning functions, particularly as a callback data parameter where the scanning process needs to track the earliest page number encountered. This information is crucial for truncation operations that need to determine safe truncation boundaries in the multixact storage files.

## Parameters / Member Variables
- `earliestExistingPage`: The page number of the earliest existing page found during directory scanning, or -1 if no page has been found yet

## Dependencies
- Functions called/Symbols referenced:
  - int64 (standard integer type for page numbers)
- Called from (representative examples):
  - [SlruScanDirCbFindEarliest](../S/SlruScanDirCbFindEarliest.md) (callback function that uses this structure to track earliest page)
  - [TruncateMultiXact](../T/TruncateMultiXact.md) (main truncation function that utilizes this structure)

## Notes and Other Information
This structure is specifically designed for callback-based directory scanning operations in the SLRU system. The earliestExistingPage field uses -1 as a sentinel value to indicate that no page has been found yet during the scanning process. The simplicity of this structure reflects its focused purpose as a data container for a specific phase of multixact truncation operations.