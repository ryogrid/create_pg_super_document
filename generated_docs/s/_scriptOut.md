# _scriptOut

## Location
src/bin/pg_dump/pg_backup_tar.c: 857 - 878

## Overview
Outputs script data to the TAR archive format by writing buffer contents to the script TAR handle.

## Definition


## Detailed Description
The  function is a specialized output handler for the TAR format archiver in pg_dump that writes script data to the TAR archive. It serves as a wrapper around the  function, specifically handling output to the script TAR handle (scriptTH) which is used for storing SQL scripts and commands within the TAR archive format.

This function is part of the TAR format implementation and provides a standardized interface for writing script content while maintaining the TAR format structure.

## Parameters / Member Variables
- : Archive handle containing the archive state and format-specific data
- : Pointer to the buffer containing data to be written
- : Number of bytes to write from the buffer

## Dependencies
- Functions called/Symbols referenced:
  - tarWrite
- Data types used:
  - lclContext
- Called from (representative examples):
  - _CloseArchive

## Notes and Other Information
- This function is specific to the TAR format archiver implementation
- Returns the number of bytes written as returned by tarWrite
- The function accesses the scriptTH (script TAR handle) from the local context
- Part of the TAR format's multi-file handling system where different types of content are written to different TAR handles