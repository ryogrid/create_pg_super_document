# gtsvector_options

## Location
[src/backend/utils/adt/tsgistidx.c:809-819](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsgistidx.c#L809-L819)

## Overview
Defines configuration options for GiST indexes on tsvector data types, specifically allowing customization of signature length for index optimization.

## Definition

```c
Datum
gtsvector_options(PG_FUNCTION_ARGS)
```
## Detailed Description
This function initializes the reloption (relation option) system for GiST tsvector indexes, allowing users to customize index behavior through storage parameters. Currently, it supports one configurable parameter: the signature length (siglen).

The signature length determines how many bytes are used to represent each tsvector as a lossy signature in the index. This is a crucial performance parameter that affects both index size and search accuracy:
- Smaller signatures use less space but may have more false positives during searches
- Larger signatures use more space but provide better selectivity and fewer false positives

The function sets up the infrastructure for PostgreSQL's CREATE INDEX ... WITH (option=value) syntax to work with GiST tsvector indexes.

## Parameters / Member Variables
- : Pointer to local_relopts structure for configuring relation options
- Returns: Void (no return value)

## Dependencies
- Functions called/Symbols referenced:
  - : Initializes the local reloptions system with specified size
  - : Adds an integer option with name, description, default, min, max, and offset
  - : Structure containing siglen field for signature length configuration
  - : Default signature length constant (31 * 4 = 124 bytes)
  - : Maximum allowed signature length constant
  - : PostgreSQL macro to return void from function
- Called from (representative examples):
  - PostgreSQL's index option processing system during CREATE INDEX operations

## Notes and Other Information
- File location: src/backend/utils/adt/tsgistidx.c:809-819
- The siglen option allows values from 1 to SIGLEN_MAX bytes, with default of 124 bytes
- This function is called during index creation when WITH clauses specify tsvector-specific options
- The GistTsVectorOptions structure is stored as part of the index's metadata
- Changing signature length requires reindexing to take effect

## Simplified Source

```c
Datum gtsvector_options(PG_FUNCTION_ARGS) {
    local_relopts *relopts = (local_relopts *) PG_GETARG_POINTER(0);

    // Initialize relation options system
    init_local_reloptions(relopts, sizeof(GistTsVectorOptions));

    // Add signature length option: default 124 bytes, range 1 to SIGLEN_MAX
    add_local_int_reloption(relopts, "siglen", "signature length",
                            SIGLEN_DEFAULT, 1, SIGLEN_MAX,
                            offsetof(GistTsVectorOptions, siglen));

    PG_RETURN_VOID();
}
```