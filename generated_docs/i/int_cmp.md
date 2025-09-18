# int_cmp

## Location
src/bin/pg_dump/pg_dump_sort.c: 1729 - 1735

## Overview
The int_cmp function is a binary heap comparator that compares two integer values for sorting operations within PostgreSQL's dump ordering system.

## Definition


## Detailed Description
This function serves as a comparator callback for binary heap operations in the topological sorting algorithm used by pg_dump. It takes two void pointers that represent integers cast to pointer types, converts them back to integer values, and performs a three-way comparison.

The function uses safe pointer-to-integer conversion through intptr_t to handle the casting properly across different architectures. It delegates the actual comparison to pg_cmp_s32, which returns -1, 0, or 1 for less-than, equal, or greater-than relationships respectively.

## Parameters / Member Variables
- : First integer value cast as void pointer
- : Second integer value cast as void pointer  
- : Unused argument parameter (required by binary heap comparator interface)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cmp_s32](../p/pg_cmp_s32.md) (PostgreSQL's signed 32-bit integer comparison function)
- Called from (representative examples):
  - [TopoSort](../T/TopoSort.md)
  - Binary heap operations in dependency sorting

## Notes and Other Information
- Used specifically for binary heap operations in topological sorting of dump objects
- Handles integer values that have been cast to void pointers for generic data structure compatibility
- Uses intptr_t for safe pointer-to-integer conversion across different architectures
- The arg parameter is unused but required by the binary heap comparator function signature
- Returns standard three-way comparison result (-1, 0, 1)
- Part of the dependency resolution and object ordering system in pg_dump
- Located in src/bin/pg_dump/pg_dump_sort.c:1729-1735