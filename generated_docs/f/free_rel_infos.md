# free_rel_infos

## Location
src/bin/pg_upgrade/info.c: 779 - 796

## Overview
Frees all relation information stored in a RelInfoArr structure, including namespace names, relation names, and tablespace names.

## Definition
```c
static void free_rel_infos(RelInfoArr *rel_arr)
```

## Detailed Description
This function performs cleanup of a RelInfoArr structure by deallocating all dynamically allocated memory associated with relation information. It iterates through each relation in the array and conditionally frees the namespace name, relation name, and tablespace name based on allocation flags. The function checks nsp_alloc and tblsp_alloc flags before freeing namespace and tablespace strings respectively, but always frees the relation name. Finally, it deallocates the relations array itself and resets the count to zero.

## Parameters / Member Variables
- `rel_arr`: Pointer to RelInfoArr structure containing relation information to be freed

## Dependencies
- Functions called/Symbols referenced:
  - pg_free
  - RelInfoArr (struct type)
- Called from (representative examples):
  - free_db_and_rel_infos

## Notes and Other Information
- This is a static function only used within src/bin/pg_upgrade/info.c
- Uses conditional freeing based on allocation flags (nsp_alloc, tblsp_alloc) to avoid freeing non-allocated strings
- Always frees relname but conditionally frees nspname and tablespace
- Sets rel_arr->nrels to 0 after cleanup
- Part of the pg_upgrade utility's memory management system for relation metadata