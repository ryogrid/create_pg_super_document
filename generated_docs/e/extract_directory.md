# extract_directory

## Location
src/bin/pg_basebackup/bbstreamer_file.c: 317 - 341

## Overview
This function creates directories during archive extraction, handling both the directory creation process and permission setting while gracefully managing cases where directories may already exist.

## Definition


## Detailed Description
The  function is responsible for creating directories as part of the archive extraction process in pg_basebackup. It performs directory creation with appropriate error handling and permission management.

The function first attempts to create the directory using  with . If the creation fails, it checks whether the failure is due to an already existing directory. In such cases, it consults  to determine if the existing directory should be tolerated (such as for PostgreSQL system directories that may have been created by other processes).

On non-Windows systems, the function also sets the directory permissions to match those specified in the archive using . This ensures that extracted directories maintain their original permission settings from the backup.

## Parameters / Member Variables
- : Path where the directory should be created
- : File permissions that should be applied to the created directory

## Dependencies
- Functions called/Symbols referenced:
  - mkdir
  - should_allow_existing_directory
  - chmod (non-Windows only)
  - pg_fatal
  - pg_dir_create_mode (global variable)
- Called from (representative examples):
  - bbstreamer_extractor_content

## Notes and Other Information
- This is a static function specific to the bbstreamer file extraction implementation
- Permission setting via  is skipped on Windows platforms due to different permission models
- The function uses  for initial directory creation, then applies archive-specific permissions
- Error handling distinguishes between creation failures due to existing directories versus other system errors
- The function integrates with PostgreSQL's error reporting system through 
- Located in src/bin/pg_basebackup/bbstreamer_file.c:317-341