# XLogArchiveIsReady

## Location
src/backend/access/transam/xlogarchive.c: 694 - 711

## Overview
Checks if an XLOG segment file has an archive notification (.ready) file, indicating it is queued for archival.

## Definition
bool XLogArchiveIsReady(const char *xlog)

## Detailed Description
XLogArchiveIsReady is a simple utility function that determines whether a WAL segment file has been marked as ready for archival by checking for the existence of a corresponding .ready file. This is the most straightforward of the archive status checking functions:

- Constructs the path to the .ready status file using StatusFilePath
- Uses stat() to check if the .ready file exists
- Returns true if the .ready file is found, false otherwise

This function provides a direct way to determine if a WAL file is currently in the archival queue waiting for the archiver process to handle it.

## Parameters / Member Variables
- : The name of the XLOG segment file to check for .ready status

## Dependencies
- Functions called/Symbols referenced:
  - StatusFilePath
- Called from (representative examples):
  - RemoveNonParentXlogFiles

## Notes and Other Information
- Most basic of the archive status checking functions
- Does not handle race conditions or check for .done files
- Specifically checks only for archival queue status (.ready files)
- Used in WAL cleanup operations to identify files pending archival
- Simple boolean check with no side effects or file creation