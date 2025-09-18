# output_set_descr

## Location
src/interfaces/ecpg/preproc/descriptor.c: 275 - 334

## Overview
Generates C code for setting descriptor items in ECPG (Embedded SQL in C for PostgreSQL), processing assignment statements and outputting appropriate ECPGset_desc function calls.

## Definition


## Detailed Description
The `output_set_descr` function is part of the ECPG preprocessor that handles SQL descriptor SET operations. It processes a global list of assignments and generates corresponding C code that calls the ECPGset_desc runtime function. The function validates descriptor items, ensuring that read-only items cannot be set and unimplemented items are properly flagged. For valid settable items (data, indicator, length, type), it generates appropriate type information and variable references in the output C code.

## Parameters / Member Variables
- `desc_name`: Name of the SQL descriptor to be set
- `index`: Index position within the descriptor (can be a variable or literal)

## Dependencies
- Functions called/Symbols referenced:
  - [find_variable](../f/find_variable.md)
  - [descriptor_item_name](../d/descriptor_item_name.md)
  - [get_dtype](../g/get_dtype.md)
  - [ECPGdump_a_type](../E/ECPGdump_a_type.md)
  - [mm_strdup](../m/mm_strdup.md)
  - [drop_assignments](../d/drop_assignments.md)
  - [whenever_action](../w/whenever_action.md)
  - mmfatal
- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- The function handles different types of descriptor items with specific validation:
  - Unimplemented items (cardinality, di_code, di_precision, precision, scale) trigger fatal errors
  - Read-only items (key_member, name, nullable, octet, ret_length, ret_octet) cannot be set
  - Settable items (data, indicator, length, type) are processed and output as ECPGset_desc calls
- Uses global `assignments` list to track descriptor assignments
- Outputs to `base_yyout` file stream as part of the preprocessing phase
- Calls `whenever_action(2 | 1)` to handle error conditions according to WHENEVER statements
- Part of the ECPG preprocessor located in src/interfaces/ecpg/preproc/descriptor.c:275-334