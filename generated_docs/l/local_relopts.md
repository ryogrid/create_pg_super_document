# local_relopts

## Location
src/include/access/reloptions.h: 165 - 170

## Overview
A structure that holds local relation option data used by build_local_reloptions() to manage extension-specific relation options and their validation.

## Definition
```c
typedef struct local_relopts
{
    List       *options;           /* list of local_relopt definitions */
    List       *validators;        /* list of relopts_validator callbacks */
    Size        relopt_struct_size; /* size of parsed bytea structure */
} local_relopts;
```

## Detailed Description
The `local_relopts` structure serves as a container for managing local relation options within PostgreSQL extensions. It maintains lists of option definitions and their associated validation callbacks, along with the total size required for the parsed bytea structure. This structure is central to PostgreSQL's extensible relation options framework, enabling extensions to define custom storage parameters independent of the core system's relation options.

## Parameters / Member Variables
- `options`: List containing local_relopt definitions that specify individual option metadata and storage offsets
- `validators`: List of relopts_validator callback functions used to validate option values during parsing
- `relopt_struct_size`: Total size in bytes needed for the parsed bytea structure that will contain all option values

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this symbol)
- Called from (representative examples):
  - [init_local_reloptions](../i/init_local_reloptions.md) (src/backend/access/common/reloptions.c:734)
  - [register_reloptions_validator](../r/register_reloptions_validator.md) (src/backend/access/common/reloptions.c:747)
  - [add_local_reloption](../a/add_local_reloption.md) (src/backend/access/common/reloptions.c:757)
  - [add_local_bool_reloption](../a/add_local_bool_reloption.md) (src/backend/access/common/reloptions.c:865)
  - [add_local_int_reloption](../a/add_local_int_reloption.md) (src/backend/access/common/reloptions.c:918)
  - [parseLocalRelOptions](../p/parseLocalRelOptions.md) (src/backend/access/common/reloptions.c:1550)
  - [build_local_reloptions](../b/build_local_reloptions.md) (src/backend/access/common/reloptions.c:1954)
  - [brin_bloom_options](../b/brin_bloom_options.md) (src/backend/access/brin/brin_bloom.c:749)
  - [index_opclass_options](../i/index_opclass_options.md) (src/backend/access/index/indexam.c:1002)

## Notes and Other Information
This structure is extensively used by PostgreSQL extensions and access methods (like BRIN, GiST) to implement custom storage parameters. The relopt_struct_size field is crucial for memory allocation and ensures that the parsed bytea structure has sufficient space for all defined options. The validators list enables custom validation logic that goes beyond basic type checking, allowing complex inter-option dependencies and business rule enforcement.