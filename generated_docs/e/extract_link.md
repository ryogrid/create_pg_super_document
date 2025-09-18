# extract_link

## Location
src/bin/pg_basebackup/bbstreamer_file.c: 342 - 354

## Overview
This function creates symbolic links during archive extraction, primarily for tablespace links in the pg_tblspc directory with support for tablespace mapping transformations.

## Definition


## Detailed Description
The  function handles the creation of symbolic links during backup restoration. While it can create any symbolic link, its primary purpose is to restore tablespace symbolic links found in the pg_tblspc directory, which point to the actual locations of PostgreSQL tablespaces.

The function accepts a potentially modified link target that may have been processed by tablespace mapping logic (via the  command-line option in pg_basebackup). This allows administrators to restore backups to different tablespace locations than those in the original database.

Although the function is designed primarily for tablespace links, the implementation doesn't restrict its use to pg_tblspc directory entries, making it a general-purpose symbolic link creator. As noted in the comments, this can be considered an "undocumented feature" for mapping any symbolic links that might exist in the data directory.

## Parameters / Member Variables
- : Path where the symbolic link should be created
- : Target path that the symbolic link should point to (may be modified by tablespace mapping)

## Dependencies
- Functions called/Symbols referenced:
  - symlink
  - pg_fatal
- Called from (representative examples):
  - bbstreamer_extractor_content

## Notes and Other Information
- This is a static function specific to the bbstreamer file extraction implementation
- The function is designed to work with tablespace mapping functionality, accepting pre-processed link targets
- No validation is performed to ensure the link is actually within pg_tblspc - the mapping is applied blindly
- Error handling uses PostgreSQL's standard error reporting mechanism through 
- The function provides a simple wrapper around the system  call with appropriate error handling
- Located in src/bin/pg_basebackup/bbstreamer_file.c:342-354