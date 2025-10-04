# server_get_sink

## Location
[src/backend/backup/basebackup_target.c:203-212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_target.c#L203-L212)

## Overview
Creates a bbsink implementation for server-side backup operations as part of PostgreSQL's base backup target system.

## Definition
static bbsink *server_get_sink(bbsink *next_sink, void *detail_arg)

## Detailed Description
This function serves as a factory method for creating server-side backup sink objects. It acts as a wrapper around bbsink_server_new(), providing a standardized interface for the base backup target system. The function is designed to be used as a callback in the BaseBackupTargetType structure, allowing the backup system to dynamically create appropriate sink implementations based on the target type.

The function is part of PostgreSQL's modular backup architecture where different backup targets (server-side, client-side, etc.) can be handled through a common interface while providing target-specific implementations.

## Parameters / Member Variables
- : The next bbsink in the chain (can be NULL if this is the terminal sink)
- : Target-specific detail argument, typically containing configuration or destination information for the backup

## Dependencies
- Functions called/Symbols referenced:
  - [bbsink_server_new](../b/bbsink_server_new.md)
  - [bbsink](../b/bbsink.md) (type)
- Called from (representative examples):
  - [BaseBackupTargetHandle](../B/BaseBackupTargetHandle.md) (via function pointer in target type structure)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the basebackup_target.c file
- The function is typically assigned to the get_sink field of a BaseBackupTargetType structure
- Located in src/backend/backup/basebackup_target.c at lines 203-212
- Part of PostgreSQL's base backup infrastructure introduced for handling different backup destinations

## Simplified Source

```c
static bbsink *server_get_sink(bbsink *next_sink, void *detail_arg) {
    // Create server-side backup sink with provided configuration
    return bbsink_server_new(next_sink, detail_arg);
}
```