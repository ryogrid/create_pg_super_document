# apply_map_update

## Location
src/backend/utils/cache/relmapper.c: 383 - 415

## Overview
A static helper function that inserts or updates a mapping between a relation OID and its file number in a given relation map, maintaining the internal mapping table used by PostgreSQL's relation mapping subsystem.

## Definition


## Detailed Description
The  function is responsible for maintaining the relation-to-file mapping table within a RelMapFile structure. It first searches for an existing mapping for the given relation OID. If found, it updates the file number for that relation. If no existing mapping is found, it creates a new mapping entry, but only if the  parameter is true. This function ensures that each relation has at most one mapping entry and prevents the mapping table from exceeding its maximum capacity.

## Parameters / Member Variables
- : Pointer to the RelMapFile structure containing the mapping table to be updated
- : The OID of the relation for which the mapping is being updated or created
- : The new RelFileNumber to be associated with the relation
- : Boolean flag indicating whether it's acceptable to add a new mapping if one doesn't exist (false means an error should be thrown if no existing mapping is found)

## Dependencies
- Functions called/Symbols referenced:
  - RelMapFile (structure)
  - RelFileNumber (type)
  - MAX_MAPPINGS (constant)
  - elog (for error reporting)
- Called from (representative examples):
  - RelationMapUpdateMap
  - merge_map_updates

## Notes and Other Information
- This is a static function, meaning it's only accessible within the relmapper.c file
- The function enforces a maximum number of mappings (MAX_MAPPINGS) to prevent unbounded growth
- Error handling includes checks for unmapped relations when add_okay is false and for exceeding the maximum mapping capacity
- The linear search through existing mappings suggests this is optimized for small numbers of mappings rather than high-performance lookups
- Part of PostgreSQL's relation mapping system that maintains the correspondence between logical relation identifiers and physical file numbers