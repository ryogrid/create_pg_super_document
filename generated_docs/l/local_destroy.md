# local_destroy

## Location
[src/bin/pg_rewind/local_source.c:184-187](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/local_source.c#L184-L187)

## Overview
Destroys and deallocates a local rewind source object by freeing its memory in pg_rewind.

## Definition
```c
static void local_destroy(rewind_source *source)
```

## Detailed Description
This function implements the `destroy` callback for the local rewind source implementation. It performs cleanup by deallocating the memory allocated for the `local_source` structure using PostgreSQL's memory management function `pfree()`. 

The function is part of the resource management lifecycle for local source objects in pg_rewind. When a local source is no longer needed, this function ensures proper cleanup to prevent memory leaks. The `local_source` structure was originally allocated using `pg_malloc0()` in the `init_local_source()` function.

## Parameters / Member Variables
- `source`: Pointer to the rewind_source structure (actually a local_source structure cast to rewind_source) to be destroyed and deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
  - [rewind_source](../r/rewind_source.md) (structure type)
- Called from (representative examples):
  - [init_local_source](../i/init_local_source.md) (assigned as function pointer at src/bin/pg_rewind/local_source.c:52)

## Notes and Other Information
- This is a static function, only accessible within local_source.c
- The function follows PostgreSQL's memory management conventions using pfree() rather than standard free()
- Part of the strategy pattern implementation where different source types implement their own cleanup logic
- The function assumes the source pointer is valid and points to memory that was allocated with PostgreSQL's memory allocation functions
- No validation is performed on the input pointer - the caller is responsible for ensuring it's valid