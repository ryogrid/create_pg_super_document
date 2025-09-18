# TAR_MEMBER

## Location
src/bin/pg_dump/pg_backup_tar.c: 76 - 88

## Overview
TAR_MEMBER is a structure that represents a member (file) within a tar archive used by PostgreSQL's pg_dump utility for backup operations.

## Definition


## Detailed Description
TAR_MEMBER is a core data structure in PostgreSQL's tar archive format implementation for pg_dump. It encapsulates all the necessary information and file handles needed to manage individual files within a tar archive during backup and restore operations. The structure maintains multiple file handles to support different operational modes and tracks the position and length of data within the archive.

## Parameters / Member Variables
- : File handle for the named file (actual content file)
- : File handle for the tar archive file
- : Temporary file handle used during operations
- : Path to the target file being processed
- : Access mode character ('r' for read, 'w' for write, etc.)
- : Current position within the file (of type pgoff_t for large file support)
- : Total length of the file (of type pgoff_t for large file support)
- : Pointer to the ArchiveHandle structure that owns this tar member

## Dependencies
- Functions called/Symbols referenced:
  - pgoff_t (PostgreSQL offset type for large files)
  - ArchiveHandle (archive management structure)
- Called from (representative examples):
  - tarOpen (opens a tar member for reading/writing)
  - tarClose (closes a tar member)
  - tarRead (reads data from a tar member)
  - tarWrite (writes data to a tar member)
  - _tarAddFile (adds a new file to the tar archive)

## Notes and Other Information
- This structure is specifically used in the tar format implementation of pg_dump's archive system
- The use of pgoff_t instead of standard offset types allows PostgreSQL to handle very large backup files
- Multiple file handles (nFH, tarFH, tmpFH) provide flexibility for different I/O operations during backup/restore
- Part of the modular archive format system in pg_dump, alongside other formats like custom and plain text