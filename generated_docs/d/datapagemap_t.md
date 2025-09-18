# datapagemap_t

## Location
src/bin/pg_rewind/datapagemap.h: 21 - 21

## Overview
A typedef alias for the datapagemap struct, providing a convenient type name for use throughout the pg_rewind codebase.

## Definition
```c
typedef struct datapagemap datapagemap_t;
```

## Detailed Description
The datapagemap_t is a type alias that provides a more convenient and conventional naming convention for the datapagemap structure. This typedef follows PostgreSQL's naming conventions by adding the '_t' suffix to indicate it's a type definition. It allows developers to use datapagemap_t instead of 'struct datapagemap' throughout the codebase, making the code more readable and maintainable. The type is extensively used in pg_rewind's API functions for handling data page mapping operations.

## Parameters / Member Variables
- Inherits all members from struct datapagemap:
  - `bitmap`: Character array serving as bitmap storage
  - `bitmapsize`: Size of the bitmap in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [datapagemap](datapagemap.md) (references the underlying struct)
- Called from (representative examples):
  - [datapagemap_iterator](datapagemap_iterator.md) (used in iterator structure)
  - [datapagemap_add](datapagemap_add.md) (function parameter)
  - [datapagemap_iterate](datapagemap_iterate.md) (function parameter and return type)
  - [datapagemap_print](datapagemap_print.md) (function parameter)
  - [file_entry_t](../f/file_entry_t.md) (used in file mapping structure)

## Notes and Other Information
- This typedef is the primary interface type used in pg_rewind's data page mapping API
- Provides type safety and code clarity compared to using the raw struct
- Located in src/bin/pg_rewind/datapagemap.h:21
- Used extensively throughout the pg_rewind module for page tracking operations