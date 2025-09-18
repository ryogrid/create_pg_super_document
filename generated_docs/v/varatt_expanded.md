# varatt_expanded

## Location
[src/include/varatt.h:74-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/varatt.h#L74-L77)

## Overview
A structure representing a TOAST pointer to an expanded object, containing a direct reference to an ExpandedObjectHeader.

## Definition


## Detailed Description
The varatt_expanded structure is a specialized TOAST pointer type designed specifically for PostgreSQL's expanded datum system. Unlike other TOAST pointer types that reference external storage (varatt_external) or indirect memory locations (varatt_indirect), varatt_expanded directly points to an ExpandedObjectHeader structure.

This structure serves as a bridge between PostgreSQL's TOAST system and the expanded object infrastructure, enabling efficient handling of complex data types that benefit from remaining in an expanded, readily-accessible format rather than being repeatedly compressed and decompressed. The expanded object system is particularly valuable for large arrays, records, and other complex data structures that undergo frequent manipulation.

The simplicity of this structure - containing only a single pointer - reflects its role as a lightweight reference mechanism. The actual functionality and data management are handled by the ExpandedObjectHeader and its associated methods, while varatt_expanded provides the necessary interface for integration with PostgreSQL's broader TOAST infrastructure.

## Parameters / Member Variables
- : A direct pointer to an ExpandedObjectHeader structure that contains the actual expanded object data and associated metadata

## Dependencies
- Functions called/Symbols referenced:
  - [ExpandedObjectHeader](../E/ExpandedObjectHeader.md)
- Called from (representative examples):
  - DatumGetEOHP
  - EOH_init_header
  - EXPANDED_POINTER_SIZE (size calculation)
  - VARTAG_SIZE (TOAST tag size calculation)

## Notes and Other Information
- Provides integration between TOAST system and expanded objects
- Simplest of the TOAST pointer structures, containing only a single pointer field
- Enables efficient handling of complex data types that benefit from expanded representation
- Works in conjunction with ExpandedObjectHeader to provide full expanded object functionality
- Part of PostgreSQL's strategy to optimize performance for frequently-accessed complex data
- The referenced ExpandedObjectHeader manages memory context and provides type-specific methods
- Used primarily for data types like arrays and records that are expensive to repeatedly decompress