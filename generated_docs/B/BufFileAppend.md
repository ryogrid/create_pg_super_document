# BufFileAppend

## Location
[src/backend/storage/file/buffile.c:905-932](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L905-L932)

## Overview
Appends the contents of a source BufFile to the end of a target BufFile by manipulating segment file lists, creating a logical concatenation while transferring ownership of underlying resources.

## Definition
```c
int64 BufFileAppend(BufFile *target, BufFile *source)
```

## Detailed Description
BufFileAppend performs a high-performance append operation by transferring ownership of the source file's underlying segment files to the target file. Rather than copying data, it manipulates the file lists within the fileset to create a logical concatenation. The operation aligns content at MAX_PHYSICAL_FILESIZE boundaries, which may create empty holes before the boundary that cannot be read by callers. After this operation, the source BufFile should never have BufFileClose called on it since its resources have been transferred to the target. Both files must be managed within the same fileset and have matching resource owners.

## Parameters / Member Variables
- `target`: The BufFile to append data to (receives ownership of source's resources)
- `source`: The BufFile whose contents will be appended (must be readOnly and not dirty)

## Dependencies
- Functions called/Symbols referenced:
  - BUFFILE_SEG_SIZE (constant)
  - repalloc
  - File (type)
  - Assert
  - elog/ERROR
- Called from (representative examples):
  - LogicalTapeImport (src/backend/utils/sort/logtape.c:639)

## Notes and Other Information
- Returns the block number within target where source content begins
- Source file must be readOnly and not dirty (clean state)
- Both files must have the same fileset and resource owner
- Creates empty holes at MAX_PHYSICAL_FILESIZE-aligned boundaries
- Transfers ownership of underlying resources from source to target
- After calling this function, never call BufFileClose on the source BufFile
- Used primarily by logical tape operations for efficient tape concatenation
- The operation works at the segment file level rather than copying actual data
- Returned block number can be used as an offset for block position calculations