# libpq_destroy

## Location
[src/bin/pg_rewind/libpq_source.c:675-685](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/libpq_source.c#L675-L685)

## Overview
Cleans up and destroys a libpq_source structure by freeing all allocated memory, but intentionally does not close the database connection.

## Definition
```c
static void libpq_destroy(rewind_source *source)
```

## Detailed Description
This function serves as the destructor for a libpq_source object. It casts the generic rewind_source pointer to the specific libpq_source type and frees all dynamically allocated memory associated with the source, including the internal arrays for paths, offsets, and lengths data, as well as the source structure itself.

Notably, this function does not close the PostgreSQL connection (PGconn) contained within the source. This design choice is documented with a comment indicating that the connection was not opened by this component, so it should not be responsible for closing it. The connection management is handled by the caller or other parts of the system.

## Parameters / Member Variables
- `source`: Pointer to rewind_source structure to be destroyed (cast internally to libpq_source)

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
- Called from (representative examples):
  - [init_libpq_source](../i/init_libpq_source.md) (src/bin/pg_rewind/libpq_source.c:96)

## Notes and Other Information
- This is a static function used internally within the pg_rewind utility
- The function follows PostgreSQL's memory management conventions using pfree instead of standard free
- The connection (PGconn) is intentionally not closed, indicating a separation of concerns where connection management is handled elsewhere
- This function should be called when the libpq_source is no longer needed to prevent memory leaks
- The function safely handles the destruction of the dynamic arrays (paths, offsets, lengths) that are used to batch file operations
- Part of the cleanup pattern for the pg_rewind utility's resource management

## Simplified Source

```c
static void
libpq_destroy(rewind_source *source)
{
    libpq_source *src = (libpq_source *) source;

    // Free all allocated memory
    pfree(src->paths.data);
    pfree(src->offsets.data);
    pfree(src->lengths.data);
    pfree(src);

    // Note: Connection is not closed here as it wasn't opened by us
}
```