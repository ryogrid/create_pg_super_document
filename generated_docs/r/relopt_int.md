# relopt_int

## Location
src/include/access/reloptions.h: 97 - 103

## Overview
relopt_int is a structure that defines an integer-type relation option, extending the base relopt_gen structure with a default value and min/max constraints.

## Definition


## Detailed Description
The relopt_int structure represents an integer relation option definition in PostgreSQL's reloption system. It inherits all the common metadata from relopt_gen and adds integer-specific fields: a default value and minimum/maximum bounds for validation.

This structure is used to define integer options that can be set on database objects. The min and max fields provide automatic range validation, ensuring that user-provided values fall within acceptable bounds. Examples might include options like "fillfactor" (with range 10-100) or "autovacuum_analyze_threshold" (with minimum 0).

## Parameters / Member Variables
- : The base relopt_gen structure containing common option metadata (name, description, kinds, lockmode, namelen, type)
- : The default integer value to use when this option is not explicitly specified by the user
- : The minimum acceptable value for this integer option (inclusive)
- : The maximum acceptable value for this integer option (inclusive)

## Dependencies
- Functions called/Symbols referenced:
  - relopt_gen
- Called from (representative examples):
  - allocate_reloption
  - init_int_reloption
  - add_int_reloption
  - add_local_int_reloption
  - parse_one_reloption
  - fillRelOptions

## Notes and Other Information
This structure enforces value constraints through the min and max fields, which are checked during option parsing. The gen field must be the first member to allow casting between relopt_int* and relopt_gen* for polymorphic handling. The type field in the embedded gen structure will be set to RELOPT_TYPE_INT for all instances of this structure.