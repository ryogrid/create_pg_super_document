# SpGistTypeDesc

## Location
src/include/access/spgist_private.h: 135 - 142

## Overview
SpGistTypeDesc is a structure that stores per-datatype information needed by SP-GiST operations, encapsulating essential type characteristics for efficient data handling.

## Definition


## Detailed Description
SpGistTypeDesc serves as a compact descriptor for data types used in SP-GiST indexes. It encapsulates the fundamental characteristics of PostgreSQL data types that are essential for proper storage, alignment, and manipulation of values within the index structure. This information is critical for the SP-GiST access method to correctly handle different data types, ensuring proper memory layout, storage efficiency, and value copying semantics.

The structure is designed to be lightweight while providing all necessary type information that SP-GiST operations need to function correctly across PostgreSQL's diverse type system.

## Parameters / Member Variables
- : OID of the PostgreSQL data type being described
- : Length of the attribute (-1 for variable-length types, positive for fixed-length)
- : Boolean indicating whether values are passed by value (true) or by reference (false)
- : Alignment requirement for the type ('c'=char, 's'=short, 'i'=int, 'd'=double)
- : Storage strategy ('p'=plain, 'e'=external, 'm'=main, 'x'=extended)

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - PostgreSQL type system primitives

- Called from (representative examples):
  - fillTypeDesc (spgutils.c:160)
  - SpGistState (spgist_private.h:150-153)
  - SpGistCache (spgist_private.h:255-258)
  - getSpGistTupleDesc (spgutils.c:309)
  - memcpyInnerDatum (spgutils.c:789)

## Notes and Other Information
- Essential component of SpGistState and SpGistCache structures
- Provides type-agnostic interface for SP-GiST operations
- Enables efficient value copying and storage layout decisions
- Supports PostgreSQL's flexible type system including variable-length and TOAST-able types
- Used extensively in tuple formation, deformation, and size calculations
- Critical for maintaining data integrity across different PostgreSQL data types in SP-GiST indexes