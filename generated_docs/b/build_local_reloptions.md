# build_local_reloptions

## Location
src/backend/access/common/reloptions.c: 1954 - 1992

## Overview
A function that builds relation options structures specifically for local (unregistered) options, creating a parsing table dynamically from the local_relopts configuration.

## Definition


## Detailed Description
This function handles the processing of local relation options that are not part of the global registered options system. It dynamically constructs a parsing table (relopt_parse_elt array) from the local_relopts structure, extracting option names, types, and offsets from the local option definitions. After creating this parsing table, it follows the standard option processing workflow: parsing the input options with parseLocalRelOptions, allocating memory with allocateReloptStruct, and filling the structure with fillRelOptions. Additionally, if validation is enabled, it runs any registered validator functions on the final result.

## Parameters / Member Variables
- : Local relation options configuration containing option definitions and validators
- : Input Datum containing the raw options to be parsed
- : Boolean flag indicating whether to validate the options and run validators

## Dependencies
- Functions called/Symbols referenced:
  - local_relopts (struct type)
  - relopt_parse_elt (struct type)
  - relopt_value (struct type)
  - local_relopt (struct type)
  - parseLocalRelOptions (function)
  - allocateReloptStruct (function)
  - fillRelOptions (function)
  - list_length (PostgreSQL list function)
  - palloc (PostgreSQL memory allocation)
  - pfree (PostgreSQL memory deallocation)
  - foreach, lfirst (PostgreSQL list macros)
  - relopts_validator (function pointer type)
- Called from:
  - index_opclass_options (src/backend/access/index/indexam.c:1038)
  - GET_STRING_RELOPTION (src/include/access/reloptions.h:233)

## Notes and Other Information
- Unlike build_reloptions, this function works with local (unregistered) options that are defined at runtime
- Dynamically constructs the parsing table from the local_relopts configuration rather than using a static table
- Supports custom validator functions that are run after option parsing when validate=true
- Memory management includes allocation of the temporary parsing elements array and its cleanup
- The local_relopts structure contains both option definitions and optional validator functions
- This function enables extensibility by allowing modules to define their own option parsing without modifying the core option registration system
- Returns a dynamically allocated options structure that becomes the caller's responsibility to manage