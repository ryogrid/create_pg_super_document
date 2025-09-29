# extendBufFile

## Location
[src/backend/storage/file/buffile.c:156-192](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L156-L192)

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
  - [OpenTemporaryFile](../O/OpenTemporaryFile.md) (creates new temporary files for standalone BufFiles)
  - [MakeNewFileSetSegment](../M/MakeNewFileSetSegment.md) (creates new segments for FileSet-managed BufFiles)
  - [repalloc](../r/repalloc.md) (reallocates memory for the expanded files array)
  - CurrentResourceOwner (global variable for resource management)
- Called from (representative examples):
  - [BufFileDumpBuffer](../B/BufFileDumpBuffer.md)

## Notes and Other Information
- This is a static function internal to buffile.c, not exposed to external modules
- The function switches resource owners temporarily to ensure the new file is owned by the BufFile's resource owner
- Uses Assert to verify that file creation succeeded (pfile >= 0)
- The files array is reallocated to accommodate the new file handle
- The numFiles counter is incremented after successfully adding the new file
- This function is typically called when write operations exceed the capacity of existing component files

## Simplified Source

```c
static void extendBufFile(BufFile *file) {
    // Temporarily switch to BufFile's resource owner
    ResourceOwner oldowner = CurrentResourceOwner;
    CurrentResourceOwner = file->resowner;

    // Create new file: temporary file or fileset segment
    File newfile;
    if (file->fileset == NULL) {
        newfile = OpenTemporaryFile(file->isInterXact);
    } else {
        newfile = MakeNewFileSetSegment(file, file->numFiles);
    }

    // Restore original resource owner
    CurrentResourceOwner = oldowner;

    // Expand files array and add new file
    file->files = repalloc(file->files, (file->numFiles + 1) * sizeof(File));
    file->files[file->numFiles] = newfile;
    file->numFiles++;
}
```