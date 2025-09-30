# initialize_reloptions

## Location
[src/backend/access/common/reloptions.c:580-682](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L580-L682)

## Overview
The initialize_reloptions function is a static initialization routine that must be called before parsing relation options, responsible for setting up the global relOpts array with all available relation option definitions.

## Definition

```c
enumRelOpts[i].gen.name;
```
## Detailed Description
This function initializes the global relOpts array, which serves as a consolidated registry of all available relation options across different data types (bool, int, real, enum, string, and custom options). The function performs two main phases:

1. **Counting Phase**: Counts the total number of relation options across all type-specific arrays (boolRelOpts, intRelOpts, realRelOpts, enumRelOpts, stringRelOpts) plus any custom options, while validating lock mode compatibility.

2. **Population Phase**: Allocates memory for the relOpts array in TopMemoryContext and populates it with pointers to all relation option definitions, setting the appropriate type and calculating name lengths for efficient parsing.

The function ensures that all relation options are accessible through a single unified interface and performs lock mode conflict assertions to validate the consistency of each option's locking requirements.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [DoLockModesConflict](../D/DoLockModesConflict.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [pfree](../p/pfree.md)
  - strlen
- Data structures accessed:
  - boolRelOpts
  - intRelOpts
  - realRelOpts
  - enumRelOpts
  - stringRelOpts
  - custom_options
  - relOpts (global array)
  - num_custom_options
  - need_initialization
- Type constants:
  - RELOPT_TYPE_BOOL
  - RELOPT_TYPE_INT
  - RELOPT_TYPE_REAL
  - RELOPT_TYPE_ENUM
  - RELOPT_TYPE_STRING
- Called from:
  - [parseRelOptions](../p/parseRelOptions.md)
  - [AlterTableGetRelOptionsLockLevel](../A/AlterTableGetRelOptionsLockLevel.md)

## Notes and Other Information
- This is a static function, only accessible within the reloptions.c file
- Memory is allocated in TopMemoryContext to ensure the relOpts array persists for the lifetime of the backend process
- The function sets need_initialization to false to prevent redundant initialization
- Lock mode compatibility is verified for each option using DoLockModesConflict assertions
- The relOpts array is null-terminated for safe iteration
- Custom options registered via add_reloption are included in the unified array

## Simplified Source

```c
static void initialize_reloptions(void) {
    int total_options = 0;

    // Count all relation options across different types
    total_options += count_options(boolRelOpts);
    total_options += count_options(intRelOpts);
    total_options += count_options(realRelOpts);
    total_options += count_options(enumRelOpts);
    total_options += count_options(stringRelOpts);
    total_options += num_custom_options;

    // Allocate memory for consolidated options array
    if (relOpts)
        pfree(relOpts);
    relOpts = MemoryContextAlloc(TopMemoryContext,
                                (total_options + 1) * sizeof(relopt_gen *));

    // Populate array with all option types
    int index = 0;
    index += populate_options(boolRelOpts, RELOPT_TYPE_BOOL, index);
    index += populate_options(intRelOpts, RELOPT_TYPE_INT, index);
    index += populate_options(realRelOpts, RELOPT_TYPE_REAL, index);
    index += populate_options(enumRelOpts, RELOPT_TYPE_ENUM, index);
    index += populate_options(stringRelOpts, RELOPT_TYPE_STRING, index);
    index += populate_custom_options(index);

    // Null-terminate the array
    relOpts[index] = NULL;

    // Mark initialization complete
    need_initialization = false;
}
```