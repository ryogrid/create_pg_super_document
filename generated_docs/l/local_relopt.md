# local_relopt

## Location
src/include/access/reloptions.h: 158 - 162

## Overview
A structure that defines a local relation option entry, mapping an option definition to its storage offset in a parsed bytea structure.

## Definition
```c
typedef struct local_relopt
{
    relopt_gen *option;         /* option definition */
    int         offset;         /* offset of parsed value in bytea structure */
} local_relopt;
```

## Detailed Description
The `local_relopt` structure represents a single entry in a local relation options registry. It serves as a mapping between a relation option definition (relopt_gen) and its storage location within a parsed bytea structure. This structure is essential for the local relation options system, which allows extensions and modules to define their own custom relation options independent of the global PostgreSQL relation options registry.

## Parameters / Member Variables
- `option`: Pointer to the relation option definition (relopt_gen structure) that describes the option's metadata, type, and behavior
- `offset`: Byte offset within the parsed bytea structure where this option's value is stored after parsing

## Dependencies
- Functions called/Symbols referenced:
  - relopt_gen (Line 160)
  - option (Line 160)
- Called from (representative examples):
  - add_local_reloption (src/backend/access/common/reloptions.c:759)
  - parseLocalRelOptions (src/backend/access/common/reloptions.c:1559)
  - build_local_reloptions (src/backend/access/common/reloptions.c:1965)

## Notes and Other Information
This structure is part of PostgreSQL's extensible relation options framework, allowing extensions to register their own custom storage parameters. The offset field enables efficient access to parsed option values in memory-mapped bytea structures, supporting fast option value retrieval during query processing. Local relation options are particularly useful for table access methods and storage engines that need custom configuration parameters.