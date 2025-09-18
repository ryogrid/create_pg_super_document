# init_local_reloptions

## Location
src/backend/access/common/reloptions.c: 734 - 746

## Overview
The init_local_reloptions function initializes a local_relopts structure for parsing relation options into a bytea structure of specified size.

## Definition


## Detailed Description
This function prepares a local_relopts structure for use in parsing and validating relation options. It initializes the structure with empty lists for options and validators, and sets the target structure size for the eventual bytea representation.

The local_relopts structure is used as a temporary workspace during option parsing, allowing access methods and other components to define their own option sets that will be parsed and validated before being serialized into a bytea format for storage.

This initialization is typically the first step in setting up custom relation option parsing for access methods, operator classes, or other PostgreSQL components that need to handle user-configurable options.

## Parameters / Member Variables
- : Pointer to the local_relopts structure to initialize
- : Size of the target structure that will hold the parsed option values

## Dependencies
- Functions called/Symbols referenced:
  - NIL (empty list constant)
- Data structures used:
  - [local_relopts](../l/local_relopts.md) (structure being initialized)
- Called from:
  - [brin_bloom_options](../b/brin_bloom_options.md)
  - [brin_minmax_multi_options](../b/brin_minmax_multi_options.md)
  - [index_opclass_options](index_opclass_options.md)
  - [gtsvector_options](../g/gtsvector_options.md)
  - GET_STRING_RELOPTION (macro)

## Notes and Other Information
- This is a public function (not static) available to other PostgreSQL modules
- The function sets up the basic structure for local option parsing without defining any specific options
- After initialization, callers typically use add_local_*_reloption functions to register specific options
- The relopt_struct_size parameter determines the size of the final parsed structure
- Both options and validators lists are initialized to NIL (empty)
- This function is commonly used by access methods and operator classes that need custom relation options
- The initialized structure serves as input to build_local_reloptions and related parsing functions