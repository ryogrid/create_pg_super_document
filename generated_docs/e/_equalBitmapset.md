# _equalBitmapset

## Location
src/backend/nodes/equalfuncs.c: 147 - 155

## Overview
A static comparison function that determines if two Bitmapset nodes are equal by delegating to the specialized bms_equal function.

## Definition


## Detailed Description
The  function provides equality comparison for Bitmapset nodes within PostgreSQL's node equality framework. Bitmapsets are specialized data structures used throughout PostgreSQL to efficiently represent sets of integers, commonly used for tracking column references, join relationships, and other set-based operations in query planning and optimization.

Rather than implementing comparison logic directly, this function delegates to the purpose-built  function from the bitmapset module. This design maintains separation of concerns and leverages the optimized comparison logic specifically designed for bitmapset data structures.

## Parameters / Member Variables
- : Pointer to the first Bitmapset to compare  
- : Pointer to the second Bitmapset to compare

Returns:  if the bitmapsets are equal,  otherwise

## Dependencies
- Functions called/Symbols referenced:
  -  (specialized bitmapset comparison function)
- Called from (representative examples):
  - [Node](../N/Node.md) equality framework (indirectly through function pointers)

## Notes and Other Information
- This function is marked as , meaning it's only accessible within the equalfuncs.c file
- Serves as a thin wrapper around the specialized  function
- Part of the custom equality checking for nodes that have the  attribute
- The underlying  function handles NULL cases correctly (two NULL bitmapsets are considered equal)
- Bitmapsets use a flexible array member structure with a word count and array of bitmap words
- The comparison performed by  includes checking word counts and then comparing each word in the bitmap arrays
- Bitmapsets are extensively used in PostgreSQL's query planner for representing sets of relations, attributes, and other entities
- The delegation pattern allows the equality framework to work with bitmapsets while keeping bitmapset-specific logic encapsulated in the bitmapset module