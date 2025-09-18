# pushState

## Location
src/backend/utils/adt/jsonb_util.c: 728 - 742

## Overview
Creates and initializes a new parse state level for the JSONB construction stack, enabling hierarchical parsing of nested containers.

## Definition
```c
static JsonbParseState *pushState(JsonbParseState **pstate)
```

## Detailed Description
This function serves as a stack management utility for JSONB parsing operations. It creates a new JsonbParseState structure and links it to the current parse state stack, effectively pushing a new level onto the stack. This is essential for handling nested JSONB containers (arrays and objects) during parsing, as each level of nesting requires its own parse state to track container-specific information like element counts, key-value pairs, and parsing options.

The function initializes the new state with default values for unique_keys and skip_nulls flags, which can be modified later based on parsing requirements. The linked list structure allows for proper stack-like behavior during parsing operations.

## Parameters / Member Variables
- `pstate`: Double pointer to the current parse state stack, allowing the function to modify the stack structure

## Dependencies
- Functions called/Symbols referenced:
  - palloc (PostgreSQL memory allocation)
  - JsonbParseState (structure type)
- Called from (representative examples):
  - pushJsonbValueScalar (when beginning new arrays or objects)

## Notes and Other Information
- This is a static function internal to jsonb_util.c, not exposed in the public API
- Memory allocation uses PostgreSQL's memory context system through palloc
- The function creates a linked list structure where each node represents a nesting level
- Default initialization sets unique_keys and skip_nulls to false
- The new state becomes the current top of the stack upon creation
- Essential for maintaining parsing context during recursive descent through nested JSONB structures