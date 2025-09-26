# RemovePgTempFilesInDir

## Location
[src/backend/storage/file/fd.c:3330-3389](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3330-L3389)

## Overview
Processes one pgsql_tmp directory to remove temporary files and directories, with options for handling missing directories and selective removal of files.

## Definition
```c
void RemovePgTempFilesInDir(const char *tmpdirname, bool missing_ok, bool unlink_all)
```

## Detailed Description
This function recursively processes a PostgreSQL temporary directory to remove files and subdirectories. It operates in two modes based on the `unlink_all` parameter:

1. **Selective mode** (`unlink_all = false`): Only removes files/directories that match the temporary name prefix (`PG_TEMP_FILE_PREFIX`)
2. **Complete removal mode** (`unlink_all = true`): Removes all contents under the directory

The function handles both files and directories appropriately - files are unlinked directly while directories are recursively processed and then removed with `rmdir()`. Error handling is implemented with LOG-level messages for various failure scenarios.

At the top level, this is typically called with `unlink_all = false` to selectively remove only temporary files, but when recursing into subdirectories, it uses `unlink_all = true` to clean up everything under temporary directories.

## Parameters / Member Variables
- `tmpdirname`: Path to the temporary directory to process
- `missing_ok`: If true, the function will not report an error if the specified directory does not exist
- `unlink_all`: If true, removes all files and directories; if false, only removes items matching the temporary file prefix

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateDir](../A/AllocateDir.md)
  - [ReadDirExtended](ReadDirExtended.md)  
  - [get_dirent_type](../g/get_dirent_type.md)
  - unlink
  - rmdir
  - [FreeDir](../F/FreeDir.md)
  - [RemovePgTempFilesInDir](RemovePgTempFilesInDir.md) (recursive call)
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md)
  - [RemovePgTempFiles](RemovePgTempFiles.md)

## Notes and Other Information
- The function uses recursive calls to handle nested directory structures
- Error handling is designed to be non-fatal - failures result in LOG messages rather than stopping execution
- The dual-flag design (missing_ok and unlink_all) provides flexibility for different cleanup scenarios
- Files that do not match the temporary prefix but are found in the directory generate unexpected file warnings
- Directory traversal skips "." and ".." entries as expected