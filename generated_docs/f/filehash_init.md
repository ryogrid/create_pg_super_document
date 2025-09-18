# filehash_init

## Location
src/bin/pg_rewind/filemap.c: 196 - 202

## Overview
Initializes the hash table used for tracking file map entries in pg_rewind operations.

## Definition


## Detailed Description
The  function creates and initializes the global hash table () that stores file map entries during pg_rewind operations. This hash table is used to track information about files and their attributes in both the target and source data directories. The function sets up the initial hash table structure using the simplehash library with a predefined initial size.

This function is part of the pg_rewind utility's file management system, which collects information about all files in the data directories before determining what actions need to be taken during the rewind process.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - filehash_create
  - FILEHASH_INITIAL_SIZE (constant set to 1000)
- Called from (representative examples):
  - main (src/bin/pg_rewind/pg_rewind.c:467)

## Notes and Other Information
- The hash table is implemented using PostgreSQL's simplehash library
- The initial size is set to 1000 entries (FILEHASH_INITIAL_SIZE)
- This function must be called before any other file hash operations
- The global  variable is used throughout the filemap.c module
- Part of the pg_rewind utility's file tracking system for database rewind operations