# ECPGset_desc

## Location
src/interfaces/ecpg/ecpglib/descriptor.c: 605 - 727

## Overview
ECPGset_desc sets descriptor item attributes by processing variable arguments containing descriptor type and data pairs, managing the creation and modification of descriptor items.

## Definition


## Detailed Description
ECPGset_desc is a variadic function that modifies or creates descriptor items within a named descriptor. It processes variable arguments consisting of descriptor type/variable pairs to set various attributes of a specific descriptor item identified by index. The function handles dynamic creation of descriptor items if they don't exist and supports setting multiple attributes including data, indicator, length, precision, scale, and type.

The function operates by:
1. Finding the named descriptor using ecpg_find_desc
2. Locating or creating the descriptor item for the specified index
3. Processing variable argument pairs of descriptor types and variables
4. Setting the appropriate attribute based on the descriptor type
5. Managing memory allocation and cleanup throughout the process

For data items, it calls ecpg_store_input to convert and format the input data, then uses set_desc_attr to configure binary/non-binary attributes. For numeric attributes (indicator, length, precision, scale, type), it uses set_int_item to store integer values.

## Parameters / Member Variables
- : Source code line number for error reporting and debugging
- : Name of the descriptor to modify
- : Index of the descriptor item to set (creates if doesn't exist)
- : Variable arguments consisting of ECPGdtype/variable pairs terminated by ECPGd_EODT

## Dependencies
- Functions called/Symbols referenced:
  - ecpg_find_desc
  - ecpg_alloc, ecpg_free
  - ecpg_store_input
  - [set_desc_attr](../s/set_desc_attr.md), set_int_item
  - [ecpg_raise](../e/ecpg_raise.md)
  - [descriptor](../d/descriptor.md), descriptor_item (struct types)
  - ECPGdtype, ECPGttype (enum types)
- Called from (representative examples):
  - ECPG test programs (sql-desc.c, sql-bytea.c)
  - SQL descriptor manipulation applications

## Notes and Other Information
- Supports descriptor types: ECPGd_data, ECPGd_indicator, ECPGd_length, ECPGd_precision, ECPGd_scale, ECPGd_type
- Creates descriptor items dynamically if they don't exist for the specified index
- Updates descriptor count when creating items with higher indices
- Manages memory for both descriptor items and temporary variable structures
- Returns false on any error condition with appropriate SQLSTATE codes
- Handles both fixed-size and dynamic arrays through arrsize/varcharsize parameters
- Critical for implementing SQL SET DESCRIPTOR functionality in ECPG applications