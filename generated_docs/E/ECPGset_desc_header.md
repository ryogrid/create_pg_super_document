# ECPGset_desc_header

## Location
src/interfaces/ecpg/ecpglib/descriptor.c: 573 - 583

## Overview
ECPGset_desc_header sets the count field in a descriptor structure, typically used to specify the number of fields or items in a SQL descriptor.

## Definition


## Detailed Description
ECPGset_desc_header is a simple utility function that updates the count field of a named descriptor. This function is typically used in ECPG applications to set the number of items or fields that the descriptor should contain. The function locates the descriptor by name and updates its count field with the provided value.

The function performs minimal validation - it only checks that the descriptor exists. If the descriptor is found, it updates the count field and returns success.

## Parameters / Member Variables
- : Source code line number for error reporting and debugging
- : Name of the descriptor to modify  
- : The count value to set in the descriptor's count field

## Dependencies
- Functions called/Symbols referenced:
  - ecpg_find_desc
  - [descriptor](../d/descriptor.md) (struct type)
- Called from (representative examples):
  - ECPG test programs (sql-desc.c)
  - SQL descriptor manipulation code

## Notes and Other Information
- Returns true on success, false if descriptor not found
- Simple setter function with minimal error checking
- The count field typically represents the number of descriptor items or fields
- Used in conjunction with other descriptor manipulation functions
- Part of the SQL descriptor management system in ECPG