# remove_target

## Location
[src/bin/pg_rewind/file_ops.c:130-155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/file_ops.c#L130-L155)

## Overview
Removes a target file, directory, or symbolic link based on the file entry's type during pg_rewind operations.

## Definition


## Detailed Description
This function is a dispatcher that removes different types of target file system objects based on their type. It validates that the file entry is marked for removal (FILE_ACTION_REMOVE) and that the target exists before proceeding. The function uses a switch statement to delegate to specific removal functions based on the target file type (regular file, directory, or symbolic link). It includes safety assertions to ensure the function is called with appropriate file entries and handles undefined file types as fatal errors.

## Parameters / Member Variables
- : Pointer to a file_entry_t structure containing file metadata and action information
  - Must have action set to FILE_ACTION_REMOVE
  - Must have target_exists set to true
  - target_type determines which removal function is called

## Dependencies
- Functions called/Symbols referenced:
  - [remove_target_dir](remove_target_dir.md) (for directories)
  - [remove_target_file](remove_target_file.md) (for regular files)
  - [remove_target_symlink](remove_target_symlink.md) (for symbolic links)
  - [pg_fatal](../p/pg_fatal.md) (for error handling)
  - Assert (for validation)
- Enums/Types used:
  - [file_entry_t](../f/file_entry_t.md) (structure type)
  - FILE_ACTION_REMOVE (action enum)
  - FILE_TYPE_DIRECTORY, FILE_TYPE_REGULAR, FILE_TYPE_SYMLINK, FILE_TYPE_UNDEFINED (file type enums)
- Called from (representative examples):
  - [perform_rewind](../p/perform_rewind.md)

## Notes and Other Information
- Part of pg_rewind utility's file operations module (src/bin/pg_rewind/file_ops.c)
- Essential for cleaning up obsolete files during PostgreSQL data directory synchronization
- Includes robust type checking and validation via assertions
- Delegates actual removal operations to specialized functions for each file type
- Fatal error handling for undefined file types ensures data integrity
- Critical component of pg_rewind's file management and cleanup operations
- Works in conjunction with the filemap system to track and process file changes