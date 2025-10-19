# local_finish_fetch

## Location
[src/bin/pg_rewind/local_source.c:176-183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/local_source.c#L176-L183)

## Overview
A no-operation implementation of the finish_fetch interface for local source operations in pg_rewind, as all fetching is done immediately by local_queue_fetch_range().

## Definition
```c
static void local_finish_fetch(rewind_source *source)
```

## Detailed Description
This function implements the `finish_fetch` callback for the local rewind source implementation. Unlike remote sources that may queue fetch operations for batch execution, the local source implementation performs all file operations immediately in `local_queue_fetch_range()`. Therefore, this function serves as a no-operation placeholder to satisfy the `rewind_source` interface contract.

The function is part of the pg_rewind utility, which synchronizes a PostgreSQL data directory with another PostgreSQL data directory, making it identical to a specified target timeline. The local source variant is used when the source data directory is on the same filesystem as the target.

## Parameters / Member Variables
- `source`: Pointer to the rewind_source structure containing the interface function pointers and source-specific data

## Dependencies
- Functions called/Symbols referenced:
  - [rewind_source](../r/rewind_source.md) (structure type)
- Called from (representative examples):
  - [init_local_source](../i/init_local_source.md) (assigned as function pointer at src/bin/pg_rewind/local_source.c:50)

## Notes and Other Information
- This is a static function, only accessible within local_source.c
- The function contains only a comment explaining why no operation is needed
- Part of the strategy pattern implementation where different source types (local vs remote) can have different behaviors
- The corresponding `local_queue_fetch_range()` function performs the actual copying immediately, eliminating the need for deferred execution

## Simplified Source

```c
static void
local_finish_fetch(rewind_source *source)
{
    // No operation needed - local_queue_fetch_range() copies immediately
}
```