# TupleDesc

## Location
src/include/access/tupdesc.h: 89 - 91

## Overview
TupleDesc is a typedef that defines a pointer to TupleDescData, serving as the standard interface for accessing tuple descriptor structures throughout PostgreSQL.

## Definition


## Detailed Description
TupleDesc is a simple but fundamental typedef that creates a pointer type to TupleDescData structures. This typedef establishes the standard interface used throughout PostgreSQL for passing and manipulating tuple descriptors. By using a pointer typedef, PostgreSQL achieves several benefits: efficient passing of large structures by reference, consistent API design, and the ability to use NULL to represent the absence of a tuple descriptor.

The typedef serves as an abstraction layer that hides the pointer nature of tuple descriptor handling from most code, making the API cleaner and more intuitive. Functions that work with tuple descriptors typically accept TupleDesc parameters and can assume they're working with valid tuple descriptor data.

## Parameters / Member Variables
- This is a typedef, not a structure, so it has no member variables
- Points to TupleDescData structure containing:
  - natts (number of attributes)
  - tdtypeid (composite type ID)
  - tdtypmod (type modifier)
  - tdrefcount (reference count)
  - constr (constraints)
  - attrs (attribute array)

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescData
- Called from (representative examples):
  - Used extensively throughout PostgreSQL codebase
  - Standard parameter type for tuple descriptor functions
  - Return type for tuple descriptor creation functions

## Notes and Other Information
- Standard interface for tuple descriptor manipulation in PostgreSQL
- Enables efficient passing of tuple descriptors by reference
- Provides clean abstraction over pointer-based tuple descriptor handling
- Can be NULL to indicate absence of tuple descriptor
- Used in virtually all functions that operate on tuple structure information
- Essential type for the PostgreSQL type system and executor