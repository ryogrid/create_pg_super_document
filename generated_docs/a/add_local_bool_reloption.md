# add_local_bool_reloption

## Location
[src/backend/access/common/reloptions.c:865-880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L865-L880)

## Overview
Adds a new boolean local reloption to a specific local reloption structure, allowing relation-specific boolean configuration options.

## Definition


## Detailed Description
This public function creates and adds a boolean reloption to a local reloption structure rather than the global registry. Local reloptions are used for relation-specific options that are not globally available but instead defined for specific access methods or relation types. The function initializes a boolean reloption with RELOPT_KIND_LOCAL and adds it to the provided local_relopts structure at the specified offset.

## Parameters / Member Variables
- : Pointer to the local_relopts structure that will contain this option
- : String name of the boolean reloption
- : Optional description string for the reloption (can be NULL)
- : The default boolean value for this reloption when not explicitly set
- : Integer offset within the reloption structure where this option's value will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [init_bool_reloption](../i/init_bool_reloption.md) (for boolean reloption initialization with RELOPT_KIND_LOCAL)
  - [add_local_reloption](add_local_reloption.md) (for adding to the local reloption list)
  - RELOPT_KIND_LOCAL (constant for local reloption type)
- Called from (representative examples):
  - Various access method implementations that need local boolean options
  - Extension code that defines relation-specific boolean configuration

## Notes and Other Information
- This is a public function, accessible from other files that include the appropriate headers
- Uses RELOPT_KIND_LOCAL to mark the option as local rather than global
- The lockmode parameter is set to 0 since local reloptions typically don't require special locking
- The offset parameter must point to a valid bool-typed field within the target structure
- Local reloptions are typically used by access methods and extensions for custom configuration