# pgstat_setup_memcxt

## Location
src/backend/utils/activity/pgstat_shmem.c: 1071 - 1083

## Overview
This static function initializes memory contexts used by PostgreSQL statistics subsystem for shared reference management and hash operations.

## Definition


## Detailed Description
The function lazily initializes two critical memory contexts for the PostgreSQL statistics system if they haven't been created yet. It uses the  macro to optimize for the common case where the contexts are already initialized. Both contexts are created as children of  with small allocation sizes, indicating they're used for lightweight operations.

The function creates:
1.  - A memory context for managing shared references to statistics entries
2.  - A memory context specifically for hash table operations involving statistics entry references

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - : Creates new memory allocation contexts
  - : Constant defining small allocation size parameters
  - : PostgreSQL's top-level memory context
  - : Compiler optimization macro for rarely taken branches
- Called from (representative examples):
  - : Called to ensure memory contexts are available before getting entry references
  - : May be called during hash table setup/operations

## Notes and Other Information
- This is a static (file-scope) function, not exposed outside pgstat_shmem.c
- Uses lazy initialization pattern - contexts are created only when first needed
- Both contexts use  indicating they handle many small allocations efficiently
- The  optimization suggests these contexts are typically initialized early and rarely need re-creation
- Part of PostgreSQL's memory management strategy for statistics infrastructure
- Memory contexts provide cleanup and organization for statistics-related allocations