# renameatt_internal

## Location
src/backend/commands/tablecmds.c: 3712 - 3856

## Overview
renameatt_internal is the core workhorse function that performs attribute (column) renaming operations, handling inheritance hierarchies, typed table dependencies, and all necessary catalog updates.

## Definition
```c
static AttrNumber renameatt_internal(Oid myrelid, const char *oldattname, const char *newattname, bool recurse, bool recursing, int expected_parents, DropBehavior behavior)
```

## Detailed Description
This comprehensive function orchestrates the complete attribute renaming process. It validates the operation through renameatt_check, handles inheritance hierarchies by recursively processing child relations when requested, manages typed table dependencies for composite types, and performs the actual catalog update in pg_attribute. The function ensures atomicity by acquiring exclusive locks and performing all-or-nothing operations across the inheritance tree. It also validates inheritance constraints and prevents renaming of system columns or inappropriately inherited attributes.

## Parameters / Member Variables
- `myrelid`: OID of the relation containing the attribute to rename
- `oldattname`: Current name of the attribute
- `newattname`: Desired new name for the attribute
- `recurse`: Whether to recursively rename in child relations
- `recursing`: Internal flag indicating this is a recursive call
- `expected_parents`: Number of expected parent relations for inheritance validation
- `behavior`: Drop behavior for handling dependencies (CASCADE, RESTRICT)

## Dependencies
- Functions called/Symbols referenced:
  - relation_open
  - renameatt_check
  - find_all_inheritors
  - find_inheritance_children
  - find_typed_table_dependencies
  - SearchSysCacheCopyAttName
  - check_for_column_name_collision
  - CatalogTupleUpdate
  - InvokeObjectPostAlterHook
  - heap_freetuple
  - relation_close
- Called from (representative examples):
  - renameatt_internal (recursive calls)
  - renameatt

## Notes and Other Information
- Returns the attribute number (attnum) of the renamed attribute
- Acquires AccessExclusiveLock on the target relation for the entire transaction
- Handles three main scenarios: inheritance hierarchies, typed table dependencies, and standalone renames
- Prevents renaming system columns (attnum <= 0) and inappropriately inherited columns
- Ensures name collision detection before applying changes
- Invokes post-alter hooks for proper event notification
- Function is static, used only within the tablecmds.c module for internal rename operations