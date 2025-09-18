# RelMapping

## Location
src/backend/utils/cache/relmapper.c: 83 - 87

## Overview
RelMapping is a simple structure that maps an OID of a catalog to its corresponding relation file number, serving as the basic mapping unit in PostgreSQL's relation mapping system.

## Definition


## Detailed Description
RelMapping represents a single mapping entry in the relation mapper system. It establishes the correspondence between a catalog's object identifier (OID) and its physical file number (RelFileNumber) on disk. This mapping is essential for PostgreSQL's ability to locate the physical files that store catalog data, particularly for critical system catalogs that need special handling during bootstrap and recovery operations.

The structure is straightforward, containing only the essential information needed to map logical catalog identifiers to their physical storage locations. This mapping is crucial for system catalogs that require stable, predictable file locations regardless of database operations that might otherwise cause relation file numbers to change.

## Parameters / Member Variables
- : The object identifier (OID) of a system catalog relation that requires mapping
- : The corresponding relation file number that identifies the physical file on disk where this catalog's data is stored

## Dependencies
- Functions called/Symbols referenced:
  - RelFileNumber (data type)
- Called from (representative examples):
  - RelMapFile (used as array element type)

## Notes and Other Information
- This structure is the fundamental building block of PostgreSQL's relation mapping system
- Used primarily for system catalogs that need predictable file locations
- The mapping helps maintain consistency during database operations that might otherwise change relation file numbers
- Part of the relmapper.c module which handles critical system catalog file mapping