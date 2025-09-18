# read_tablespace_map

## Location
src/backend/access/transam/xlogrecovery.c: 1354 - 1457

## Overview
Reads and parses the tablespace_map file during backup recovery to extract tablespace OID-to-path mappings needed for creating proper symlinks in the restored database.

## Definition


## Detailed Description
This function checks for the presence of a tablespace_map file during recovery from a backup dump. When found, it parses the file to extract tablespace information including OID and path mappings. The tablespace_map file is created during backup operations and contains the necessary information to recreate tablespace symlinks in the correct locations during recovery.

The function performs line-by-line parsing of the tablespace_map file format, which contains entries with tablespace OIDs followed by their corresponding filesystem paths. Each line is processed to extract the OID (converted from string to unsigned long) and the associated path string. The function handles backslash escaping within paths and validates the format of each entry.

The parsed tablespace information is returned as a list of tablespaceinfo structs, each containing the OID and path for a single tablespace. This information is later used by the recovery process to create appropriate symlinks.

## Parameters / Member Variables
- : Output parameter - pointer to a List that will be populated with tablespaceinfo structs for each tablespace found in the map file

## Dependencies
- Functions called/Symbols referenced:
  - AllocateFile (opens tablespace_map file for reading)
  - FreeFile (closes the tablespace_map file)
  - palloc0 (allocates zeroed memory for tablespaceinfo structs)
  - pstrdup (duplicates path strings)
  - lappend (appends tablespaceinfo to the list)
  - TABLESPACE_MAP (tablespace_map filename constant)
  - tablespaceinfo (struct type for storing tablespace OID and path)
- Called from:
  - InitWalRecovery (during WAL recovery initialization)

## Notes and Other Information
- Returns false if tablespace_map file doesn't exist (normal case for non-backup recovery)
- Returns true if tablespace_map found and parsed successfully  
- File format expects OID followed by exactly one space followed by path
- Handles backslash escaping within file paths for proper de-escaping
- Validates OID conversion and issues FATAL error for malformed entries
- Each line must be properly terminated or parsing fails with FATAL error
- The parsing is intentionally crude but sufficient for the fixed format produced by backup tools
- Memory for tablespaceinfo structs is allocated in the current memory context