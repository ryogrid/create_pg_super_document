# BufFileSize

## Location
[src/backend/storage/file/buffile.c:866-904](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L866-L904)

## Overview
Returns the total size of a fileset-based BufFile by calculating the combined size across all component files, including any holes created by BufFileAppend operations.

## Definition
```c
int64 BufFileSize(BufFile *file)
```

## Detailed Description
BufFileSize calculates the total logical size of a BufFile that is backed by a fileset. It does this by multiplying the number of complete files by MAX_PHYSICAL_FILESIZE and adding the size of the last (possibly incomplete) file. The function includes any holes that may have been left behind by BufFileAppend operations as part of the total size calculation. The function requires that the BufFile be fileset-based (file->fileset != NULL) and will report an error if it cannot determine the size of the underlying files.

## Parameters / Member Variables
- `file`: Pointer to the BufFile structure whose size is to be determined (must be fileset-based)

## Dependencies
- Functions called/Symbols referenced:
  - [FileSize](../F/FileSize.md)
  - [FilePathName](../F/FilePathName.md)
  - MAX_PHYSICAL_FILESIZE (constant)
  - Assert
  - ereport/ERROR
- Called from (representative examples):
  - [LogicalTapeImport](../L/LogicalTapeImport.md) (src/backend/utils/sort/logtape.c:625)

## Notes and Other Information
- Only works with fileset-based BufFiles (requires file->fileset != NULL)
- Includes holes left by BufFileAppend operations in the size calculation
- Reports errors via ereport() if file size cannot be determined
- The size calculation accounts for PostgreSQL's file segmentation where individual files are limited to MAX_PHYSICAL_FILESIZE bytes
- Used primarily by logical tape operations for determining tape sizes during import operations
- The function provides the logical size that includes any sparse regions in the file

## Simplified Source

```c
int64
BufFileSize(BufFile *file)
{
    int64 lastFileSize;

    Assert(file->fileset != NULL);

    // Get size of the last segment file
    lastFileSize = FileSize(file->files[file->numFiles - 1]);
    if (lastFileSize < 0)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not determine size of temporary file \"%s\" from BufFile \"%s\": %m",
                        FilePathName(file->files[file->numFiles - 1]),
                        file->name)));

    // Calculate total size: (complete segments * max size) + last segment size
    return ((file->numFiles - 1) * (int64) MAX_PHYSICAL_FILESIZE) +
           lastFileSize;
}
```