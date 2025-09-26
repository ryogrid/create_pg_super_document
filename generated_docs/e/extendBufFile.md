# extendBufFile

## Location
src/backend/storage/file/buffile.c: 156 - 192

## Overview
Adds a new component file to an existing BufFile, expanding its capacity when the current files become full.

## Definition
```c
static void extendBufFile(BufFile *file)
```

## Detailed Description
extendBufFile extends a BufFile by adding another component file to handle additional data beyond the capacity of existing files. The function handles two scenarios: for standalone temporary files, it creates a new temporary file using OpenTemporaryFile; for FileSet-managed files, it creates a new segment using MakeNewFileSetSegment.

The function carefully manages resource ownership by temporarily switching to the BufFile's resource owner during file creation, ensuring the new file is properly associated with the correct resource context. After creating the new file, it expands the files array using repalloc and adds the new file handle to the end of the array.

## Parameters / Member Variables
- `file`: Pointer to the BufFile structure to be extended with an additional component file

## Dependencies
- Functions called/Symbols referenced:
  - OpenTemporaryFile (creates new temporary files for standalone BufFiles)
  - MakeNewFileSetSegment (creates new segments for FileSet-managed BufFiles)
  - repalloc (reallocates memory for the expanded files array)
  - CurrentResourceOwner (global variable for resource management)
- Called from (representative examples):
  - BufFileDumpBuffer

## Notes and Other Information
- This is a static function internal to buffile.c, not exposed to external modules
- The function switches resource owners temporarily to ensure the new file is owned by the BufFile's resource owner
- Uses Assert to verify that file creation succeeded (pfile >= 0)
- The files array is reallocated to accommodate the new file handle
- The numFiles counter is incremented after successfully adding the new file
- This function is typically called when write operations exceed the capacity of existing component files