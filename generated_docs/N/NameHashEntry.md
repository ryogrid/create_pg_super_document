# NameHashEntry

## Location
src/backend/utils/adt/ruleutils.c: 307 - 527

## Overview
NameHashEntry is a hash table entry structure used in PostgreSQL's rule utility system to track table name usage and generate unique aliases during SQL deparsing operations.

## Definition

```c
typedef void (*rsv_callback) (Node *node, deparse_context *context,
							  void *callback_arg);
```
## Detailed Description
The NameHashEntry structure serves as a hash table entry for managing table name uniqueness during SQL deparsing operations in ruleutils.c. This structure is specifically used by the set_rtable_names() function to ensure that table aliases are unique when reconstructing SQL text from PostgreSQL's internal query tree representation.

The structure implements a simple naming collision resolution mechanism where each base name gets tracked along with a counter representing the highest numeric suffix used so far. When name conflicts arise, the counter is incremented to generate unique variants (e.g., "table", "table_1", "table_2").

The name field serves as both the hash key for lookup operations and the base name for generating unique variants. The requirement that it be the first field in the structure follows PostgreSQL's hash table implementation conventions for efficient key-based lookups.

## Parameters / Member Variables
- : Fixed-size character array containing the base table name that serves as the hash key; must be the first field for hash table compatibility
- : Integer tracking the highest numeric suffix used for this base name to ensure unique alias generation

## Dependencies
- Functions called/Symbols referenced:
  - deparse_context (used in conjunction with deparsing operations)
  - NAMEDATALEN (PostgreSQL constant defining maximum identifier length)

- Called from (representative examples):
  - set_rtable_names (primary user for name collision detection and resolution)

## Notes and Other Information
- This structure is specifically designed for PostgreSQL's hash table implementation requirements
- The name field being first allows the hash table to directly use the structure as a key
- Used internally by the rule system for generating human-readable SQL from parsed query trees
- Counter mechanism provides a simple but effective way to resolve naming conflicts
- Part of the broader deparse_context ecosystem for SQL reconstruction
- Essential for ensuring generated SQL maintains proper table reference semantics