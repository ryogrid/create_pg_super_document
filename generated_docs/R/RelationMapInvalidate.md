# RelationMapInvalidate

## Location
[src/backend/utils/cache/relmapper.c:468-489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L468-L489)

## Overview
A public function that handles shared invalidation (SI) cache flush messages by selectively reloading relation mapping files when they are currently valid.

## Definition


## Detailed Description
The  function is part of PostgreSQL's shared invalidation system, responsible for maintaining cache consistency across multiple backend processes. When an SI cache flush message is received, this function determines whether to reload the appropriate relation mapping file (shared or local). It includes an important safety mechanism: it only attempts to reload a mapping file if it's currently loaded and valid (indicated by the magic number). This prevents issues in processes like the autovacuum launcher that should not attempt to read local maps since they're not attached to any particular database.

## Parameters / Member Variables
- : Boolean flag indicating whether to invalidate the shared mapping file (true) or the local mapping file (false)

## Dependencies
- Functions called/Symbols referenced:
  - RELMAPPER_FILEMAGIC (constant)
  - [load_relmap_file](../l/load_relmap_file.md) (function)
  - shared_map (global variable)
  - local_map (global variable)
- Called from (representative examples):
  - [LocalExecuteInvalidationMessage](../L/LocalExecuteInvalidationMessage.md)
  - MinSizeOfRelmapUpdate (referenced in header)

## Notes and Other Information
- This is a public function, accessible from other parts of PostgreSQL's codebase
- Critical part of PostgreSQL's cache invalidation infrastructure that ensures consistency across multiple backend processes
- The magic number check prevents unnecessary or dangerous file loading attempts in processes that haven't loaded the mapping files
- Handles both shared and local relation mapping files depending on the parameter
- Essential for maintaining data consistency when relation mappings change and need to be propagated to all active backends
- Works in conjunction with the shared invalidation message system to ensure all processes have up-to-date relation mappings