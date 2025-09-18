# isRelDataFile

## Location
src/bin/pg_rewind/filemap.c: 570 - 652

## Overview
Determines whether a given file path represents a PostgreSQL relation data file by parsing the path format and validating it against known tablespace and database directory structures.

## Definition
static bool isRelDataFile(const char *path)

## Detailed Description
This function analyzes a file path to determine if it represents a PostgreSQL relation data file belonging to the main fork. It recognizes three possible directory structures where relation data files can be stored:

1. **Global tablespace**:  - for shared relations
2. **Default tablespace**:  - for regular relations in the default tablespace  
3. **Custom tablespace**:  - for relations in non-default tablespaces

The function uses sscanf to parse the path and extract the tablespace OID, database OID, relation file number, and segment number. It only considers files from the main fork as relation data files, since WAL only contains block references for the main fork, making it impossible to reliably track changes to other forks.

After parsing, it performs a cross-validation step by reconstructing the expected path using datasegpath() and comparing it to the original path to eliminate false matches from files with extra characters.

## Parameters / Member Variables
- path: The file path to analyze for relation data file patterns

## Dependencies
- Functions called/Symbols referenced:
  - RelFileLocator (struct type)
  - InvalidOid
  - InvalidRelFileNumber  
  - GLOBALTABLESPACE_OID
  - DEFAULTTABLESPACE_OID
  - TABLESPACE_VERSION_DIRECTORY
  - datasegpath
  - MAIN_FORKNUM
  - strcmp
  - pfree
- Called from (representative examples):
  - insert_filehash_entry
  - process_source_file

## Notes and Other Information
- This is a static function, only visible within the filemap.c compilation unit
- Only considers main fork files as relation data files; other forks are always copied in full
- Performs thorough validation to avoid false positives by reconstructing the expected path
- Handles segmented files (files split into multiple segments due to size limits)
- Part of pg_rewind's file classification system that determines how different types of files should be handled during the rewind process
- The cross-validation step using datasegpath ensures that files with similar but incorrect naming patterns are not mistakenly identified as relation files