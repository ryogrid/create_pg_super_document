# AttInMetadata

## Location
src/include/funcapi.h: 35 - 48

## Overview
A structure that holds metadata information needed to efficiently convert raw C strings into PostgreSQL tuples, particularly useful for set-returning functions (SRFs) and composite type construction.

## Definition


## Detailed Description
AttInMetadata is a support structure designed to ease the creation of composite types from raw C string data. It pre-computes and caches expensive metadata lookups that would otherwise be performed repeatedly during tuple construction. This structure is particularly valuable in set-returning functions where the same tuple structure is created many times.

The structure contains a complete TupleDesc along with pre-computed arrays of input functions, I/O parameters, and type modifiers for each attribute. This design avoids redundant CPU cycles that would be spent looking up this information on each function call, significantly improving performance for functions that create many tuples.

## Parameters / Member Variables
- : Complete TupleDesc describing the structure of tuples to be created
- : Array of FmgrInfo structures containing pre-looked-up input functions for each attribute type
- : Array of OIDs representing I/O parameter types for each attribute 
- : Array of int32 values containing type modifiers for each attribute (e.g., precision for numeric types)

## Dependencies
- Functions called/Symbols referenced:
  - TupleDesc (struct type)
  - FmgrInfo (struct type)
  - Oid (type alias)
- Called from (representative examples):
  - TupleDescGetAttInMetadata
  - BuildTupleFromCStrings
  - FuncCallContext (as a member)
  - show_all_settings
  - pltcl_build_tuple_result
  - libpqrcv_processTuples

## Notes and Other Information
- Designed to optimize performance in set-returning functions by pre-computing expensive metadata
- Commonly used in conjunction with BuildTupleFromCStrings for efficient tuple construction
- Essential component of the PostgreSQL function API for handling composite types
- Used extensively in procedural languages (like PL/Tcl) and system functions
- The structure is typically initialized once and reused across multiple tuple creations
- Part of PostgreSQL's funcapi.h infrastructure for supporting user-defined functions