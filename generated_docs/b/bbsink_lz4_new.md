# bbsink_lz4_new

## Location
[src/backend/backup/basebackup_lz4.c:62-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_lz4.c#L62-L92)

## Overview
Creates a new base backup sink that performs LZ4 compression for PostgreSQL base backups.

## Definition

```c
bbsink *
bbsink_lz4_new(bbsink *next, pg_compress_specification *compress)
```
## Detailed Description
This function creates and initializes a new LZ4 compression sink for base backup operations. It allocates memory for a  structure, sets up the operation callbacks to the LZ4-specific operations table (), and configures the compression level based on the provided compression specification. The function implements a chain of responsibility pattern where this LZ4 sink can be linked to the next sink in the backup pipeline.

The function includes compile-time conditional compilation - if PostgreSQL is built without LZ4 support ( not defined), it will report an error and return NULL.

## Parameters / Member Variables
- : The next sink in the backup pipeline chain
- : Compression specification containing the compression level and other settings

## Dependencies
- Functions called/Symbols referenced:
  - ereport (error reporting)
  - [palloc0](../p/palloc0.md) (memory allocation)
  - Assert (assertion checking)
- Called from (representative examples):
  - [SendBaseBackup](../S/SendBaseBackup.md) (src/backend/backup/basebackup.c:1044)
  - [bbsink_cleanup](bbsink_cleanup.md) (src/include/backup/basebackup_sink.h:288)

## Notes and Other Information
- Requires LZ4 library support at compile time ( macro)
- Validates compression level is within valid range (0-12)
- Returns pointer to the base bbsink structure, enabling polymorphic behavior
- Part of the pluggable backup sink architecture introduced for flexible backup formats