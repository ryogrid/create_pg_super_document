# init_bool_reloption

## Location
src/backend/access/common/reloptions.c: 832 - 848

## Overview
Allocates and initializes a new boolean reloption structure with type-specific default value configuration.

## Definition


## Detailed Description
This static function creates a new boolean reloption by calling allocate_reloption with RELOPT_TYPE_BOOL and then setting the boolean-specific default value. It serves as a specialized constructor for boolean reloptions, handling both the general reloption initialization and the boolean-specific field setup.

## Parameters / Member Variables
- : A bits32 value specifying the kinds of relations this option applies to
- : String name of the boolean reloption
- : Optional description string for the reloption (can be NULL)
- : The default boolean value for this reloption
- : The lock mode required when setting this reloption

## Dependencies
- Functions called/Symbols referenced:
  - allocate_reloption (for basic reloption allocation and initialization)
  - RELOPT_TYPE_BOOL (constant for boolean type specification)
- Called from (representative examples):
  - add_bool_reloption
  - add_local_bool_reloption

## Notes and Other Information
- This is a static function, only accessible within the reloptions.c file
- The function is a thin wrapper around allocate_reloption with boolean-specific initialization
- It casts the returned generic relopt_gen pointer to the more specific relopt_bool type
- The default_val parameter allows setting the boolean default value for the reloption