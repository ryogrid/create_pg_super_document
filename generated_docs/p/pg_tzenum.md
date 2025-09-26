# pg_tzenum

## Location
src/timezone/pgtz.c: 385 - 396

## Overview
The  struct is an internal data structure used for enumerating available timezone names by recursively traversing timezone directory structures in PostgreSQL's timezone system.

## Definition


## Detailed Description
The  structure serves as a directory traversal state machine for enumerating all available timezone names in PostgreSQL. It maintains a stack-based approach to recursively explore timezone directories, allowing for deep directory structures while preventing infinite recursion through the  limit (set to 10).

The structure is designed to be used with the timezone enumeration functions (, , ) that provide a safe iterator pattern for walking through the timezone directory hierarchy. The embedded  member serves as a working buffer for constructing timezone information as directories are traversed.

This struct is primarily used internally by PostgreSQL's timezone system and is not intended for direct external manipulation. It's most notably used by the  SQL function to provide a list of all available timezone names.

## Parameters / Member Variables
- : Length of the base timezone directory path, used to calculate relative timezone names by stripping the base path prefix
- : Current depth in the directory traversal stack (0-based), indicating how deep into subdirectories the enumeration has progressed  
- : Array of open directory descriptors (DIR*) for each level of the directory stack, allowing concurrent traversal of multiple directory levels
- : Array of directory name strings corresponding to each level in the traversal stack, used to construct full timezone paths
- : Working  structure used as a buffer for timezone information during enumeration, contains the canonically-cased timezone name and state information

## Dependencies
- Functions called/Symbols referenced:
  -  (constant set to 10)
  -  (embedded timezone structure)
  -  (system directory descriptor type)
  -  (maximum timezone name length constant)

- Called from (representative examples):
  -  (creates and initializes the structure)
  -  (traverses using the structure)
  -  (cleans up the structure)
  -  (SQL function that uses this for timezone enumeration)

## Notes and Other Information
- The structure is allocated using  and should be freed with 
- The  constant prevents infinite recursion and stack overflow when traversing deeply nested directory structures
- Data returned by  points into this structure and is only valid until the next call
- The structure is opaque outside the timezone library and is typedef'd in 
- All directory and string allocations within the structure use PostgreSQL's palloc memory context system
- The traversal is designed to skip hidden files (those starting with '.') and only process regular timezone data files