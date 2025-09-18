# convert_link_to_directory

## Location
src/backend/backup/basebackup.c: 2094 - 2110

## Overview
Converts symbolic link entries to directory entries in file stat structures during base backup operations, ensuring proper handling of symlinked directories.

## Definition
```c
static void convert_link_to_directory(const char *pathbuf, struct stat *statbuf)
```

## Detailed Description
This function modifies the file mode in a stat structure to treat symbolic links as directories during backup operations. When PostgreSQL encounters a symbolic link that points to a directory (such as tablespace links), it needs to archive the contents as if it were a regular directory rather than preserving the symbolic link itself. The function changes the st_mode field from a symbolic link mode to a directory mode with appropriate permissions.

## Parameters / Member Variables
- `pathbuf`: Path buffer containing the file path (currently unused in implementation but kept for potential future use)
- `statbuf`: Pointer to stat structure that will be modified to represent a directory instead of a symbolic link

## Dependencies
- Functions called/Symbols referenced:
  - S_ISLNK (macro to test if file mode represents a symbolic link)
  - S_IFDIR (file type constant for directories)
  - pg_dir_create_mode (PostgreSQL directory creation mode)
- Called from (representative examples):
  - [sendDir](../s/sendDir.md)

## Notes and Other Information
- Static function used only within the basebackup.c module
- Essential for proper handling of tablespace symbolic links during backup
- Ensures that symlinked directories are backed up as actual directories with their contents
- The pathbuf parameter is currently unused but maintained for interface consistency
- Uses PostgreSQL's standard directory creation mode for the converted entry