# SH_TYPE

## Location
src/include/lib/simplehash.h: 145 - 173

## Overview
SH_TYPE is a macro that generates the type name for specialized hash table implementations in PostgreSQL's templated simplehash system.

## Definition
```c
#define SH_TYPE SH_MAKE_NAME(hash)
```

## Detailed Description
SH_TYPE is a fundamental macro in PostgreSQL's simplehash.h templating system that creates the main hash table type name for a specific instantiation. When used with a prefix (defined by SH_PREFIX), it generates a type name following the pattern `<prefix>_hash`. This macro is part of a sophisticated code generation system that creates specialized, high-performance open-addressing hash tables tailored to specific data types.

The simplehash system is designed to generate "templated" hash table implementations that offer significant performance improvements over dynahash by eliminating indirect function calls, providing better CPU cache behavior through open addressing, and offering type safety. The SH_TYPE macro specifically defines the main hash table structure that contains all the essential components: size information, member count, hash buckets, memory context, and user-defined data.

The generated hash table structure includes a 64-bit size field to handle very large tables, membership counters, size masks for efficient bucket calculations, growth thresholds for dynamic resizing, and the actual data array containing hash elements. This design supports Robin Hood hashing with linear probing for optimal performance characteristics.

## Parameters / Member Variables
- No direct parameters (macro definition)
- When instantiated, the resulting SH_TYPE structure contains:
  - `size`: 64-bit size of data/bucket array to handle UINT32_MAX sized hash tables
  - `members`: Number of elements with valid contents
  - `sizemask`: Mask for bucket and size calculations based on size
  - `grow_threshold`: Boundary after which to grow hashtable
  - `data`: Hash buckets array of SH_ELEMENT_TYPE
  - `ctx`: Memory context for allocations (unless SH_RAW_ALLOCATOR is defined)
  - `private_data`: User-defined data useful for callbacks

## Dependencies
- Functions called/Symbols referenced:
  - SH_MAKE_NAME
- Called from (representative examples):
  - SH_CREATE
  - SH_DESTROY
  - SH_RESET
  - SH_INSERT
  - SH_LOOKUP
  - SH_DELETE
  - SH_START_ITERATE
  - All major simplehash operations

## Notes and Other Information
- Part of the templated hash table generation system in src/include/lib/simplehash.h:106
- Must be used in conjunction with SH_PREFIX to generate meaningful type names
- The resulting type represents the main hash table structure for a specific instantiation
- Used extensively throughout PostgreSQL for creating specialized hash tables with optimal performance
- The design supports both memory context and raw allocator modes
- Critical for type safety in the templated hash table system