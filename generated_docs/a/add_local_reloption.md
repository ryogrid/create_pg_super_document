# add_local_reloption

## Location
src/backend/access/common/reloptions.c: 757 - 774

## Overview
Adds an already-created custom reloption to the local list of reloptions for a relation.

## Definition


## Detailed Description
This is a static helper function that manages the addition of custom reloptions to a local reloption structure. It allocates memory for a new local_relopt structure, initializes it with the provided option and offset, and appends it to the list of options in the local_relopts structure. The function includes an assertion to ensure the offset is within the bounds of the reloption structure size.

## Parameters / Member Variables
- : Pointer to the local_relopts structure that maintains the list of reloptions
- : Pointer to the relopt_gen structure representing the reloption to be added
- : Integer offset within the reloption structure where this option's value will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - Assert (assertion macro)
  - lappend (list append function)
- Called from (representative examples):
  - [add_local_bool_reloption](add_local_bool_reloption.md)
  - [add_local_int_reloption](add_local_int_reloption.md)
  - [add_local_real_reloption](add_local_real_reloption.md)
  - [add_local_enum_reloption](add_local_enum_reloption.md)
  - [add_local_string_reloption](add_local_string_reloption.md)

## Notes and Other Information
- This is a static function, only accessible within the reloptions.c file
- The function performs bounds checking via Assert to ensure the offset is valid
- Memory for the local_relopt structure is allocated using palloc
- The function extends the options list in the local_relopts structure using lappend