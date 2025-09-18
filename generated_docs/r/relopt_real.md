# relopt_real

## Location
src/include/access/reloptions.h: 105 - 111

## Overview
relopt_real is a structure that defines a floating-point (real) type relation option, extending the base relopt_gen structure with a default value and min/max constraints.

## Definition


## Detailed Description
The relopt_real structure represents a floating-point relation option definition in PostgreSQL's reloption system. It inherits all the common metadata from relopt_gen and adds real-number-specific fields: a default value and minimum/maximum bounds for validation.

This structure is used to define floating-point options that can be set on database objects. The min and max fields provide automatic range validation, ensuring that user-provided values fall within acceptable bounds. Examples might include options like "random_page_cost" or "seq_page_cost" which accept decimal values for cost estimation.

## Parameters / Member Variables
- : The base relopt_gen structure containing common option metadata (name, description, kinds, lockmode, namelen, type)
- : The default double-precision floating-point value to use when this option is not explicitly specified by the user
- : The minimum acceptable value for this real option (inclusive)
- : The maximum acceptable value for this real option (inclusive)

## Dependencies
- Functions called/Symbols referenced:
  - [relopt_gen](relopt_gen.md)
- Called from (representative examples):
  - [allocate_reloption](../a/allocate_reloption.md)
  - [init_real_reloption](../i/init_real_reloption.md)
  - [add_real_reloption](../a/add_real_reloption.md)
  - [add_local_real_reloption](../a/add_local_real_reloption.md)
  - [parse_one_reloption](../p/parse_one_reloption.md)
  - [fillRelOptions](../f/fillRelOptions.md)

## Notes and Other Information
This structure enforces value constraints through the min and max fields, which are checked during option parsing to ensure floating-point values are within valid ranges. The gen field must be the first member to allow casting between relopt_real* and relopt_gen* for polymorphic handling. The type field in the embedded gen structure will be set to RELOPT_TYPE_REAL for all instances of this structure.