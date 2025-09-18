# create_target

## Location
src/bin/pg_rewind/file_ops.c: 156 - 186

## Overview
Creates target directories or symbolic links based on the source file type during pg_rewind operations.

## Definition


## Detailed Description
This function is a dispatcher that creates different types of target file system objects based on their source type. It validates that the file entry is marked for creation (FILE_ACTION_CREATE) and that the target does not already exist before proceeding. The function uses a switch statement to delegate to specific creation functions based on the source file type. Notably, it explicitly excludes regular files from creation through this function, as regular files are handled by open_target_file instead. It includes safety assertions and handles undefined file types as fatal errors.

## Parameters / Member Variables
- : Pointer to a file_entry_t structure containing file metadata and action information
  - Must have action set to FILE_ACTION_CREATE
  - Must have target_exists set to false
  - source_type determines which creation function is called
  - For symlinks, source_link_target provides the link target path

## Dependencies
- Functions called/Symbols referenced:
  - [create_target_dir](create_target_dir.md) (for directories)
  - [create_target_symlink](create_target_symlink.md) (for symbolic links)
  - [pg_fatal](../p/pg_fatal.md) (for error handling)
  - Assert (for validation)
- Enums/Types used:
  - [file_entry_t](../f/file_entry_t.md) (structure type)
  - FILE_ACTION_CREATE (action enum)
  - FILE_TYPE_DIRECTORY, FILE_TYPE_SYMLINK, FILE_TYPE_REGULAR, FILE_TYPE_UNDEFINED (file type enums)
- Called from (representative examples):
  - [perform_rewind](../p/perform_rewind.md)

## Notes and Other Information
- Part of pg_rewind utility's file operations module (src/bin/pg_rewind/file_ops.c)
- Essential for creating necessary directory structure and symbolic links during PostgreSQL data directory synchronization
- Explicitly excludes regular file creation (handled by open_target_file/write_target_range)
- Includes robust type checking and validation via assertions
- Delegates actual creation operations to specialized functions for each supported file type
- Fatal error handling for unsupported operations and undefined file types ensures data integrity
- Critical component of pg_rewind's file management and structure recreation operations
- Works in conjunction with the filemap system to track and process required file creations