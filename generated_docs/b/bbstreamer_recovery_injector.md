# bbstreamer_recovery_injector

## Location
[src/bin/pg_basebackup/bbstreamer_inject.c:18-27](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_inject.c#L18-L27)

## Overview
A specialized bbstreamer implementation that injects recovery configuration files into a PostgreSQL base backup archive stream during pg_basebackup operations.

## Definition
```c
typedef struct bbstreamer_recovery_injector
{
    bbstreamer      base;
    bool            skip_file;
    bool            is_recovery_guc_supported;
    bool            is_postgresql_auto_conf;
    bool            found_postgresql_auto_conf;
    PQExpBuffer     recoveryconfcontents;
    bbstreamer_member member;
} bbstreamer_recovery_injector;
```

## Detailed Description
The `bbstreamer_recovery_injector` is a concrete implementation of the bbstreamer interface designed to modify tar archive streams during PostgreSQL base backup operations. It intelligently injects recovery configuration into the backup based on the target PostgreSQL version:

- **For PostgreSQL 12+**: Creates or modifies `postgresql.auto.conf` with recovery parameters and injects an empty `standby.signal` file
- **For older versions**: Injects a `recovery.conf` file with the specified recovery configuration

The streamer processes the archive stream chunk by chunk, intercepting relevant files and either skipping them (to be replaced) or modifying their content before forwarding to the next streamer in the chain.

## Parameters / Member Variables
- `base`: Base bbstreamer structure containing common streaming operations and next streamer reference
- `skip_file`: Flag indicating whether the current file being processed should be skipped (not forwarded to next streamer)
- `is_recovery_guc_supported`: Boolean indicating if the target PostgreSQL version supports recovery GUCs (version 12+)
- `is_postgresql_auto_conf`: Flag set when currently processing the postgresql.auto.conf file
- `found_postgresql_auto_conf`: Tracks whether postgresql.auto.conf was found in the archive to determine if it needs to be created
- `recoveryconfcontents`: Buffer containing the recovery configuration content to be injected
- `member`: Copy of the current archive member being processed, allowing for modifications

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md) (base structure)
  - bbstreamer_member (member structure)
  - PQExpBuffer (PostgreSQL string buffer type)
- Called from (representative examples):
  - [bbstreamer_recovery_injector_new](bbstreamer_recovery_injector_new.md) (constructor function)
  - [bbstreamer_recovery_injector_content](bbstreamer_recovery_injector_content.md) (content processing function)

## Notes and Other Information
- This struct is part of the pg_basebackup utilitys streaming backup infrastructure
- The recovery injection strategy differs based on PostgreSQL version compatibility
- Files like `standby.signal` and `recovery.conf` are handled specially - existing versions are skipped and new ones are injected at archive trailer
- The struct maintains state across multiple chunk processing calls to track file discovery and content modification
- Located in `src/bin/pg_basebackup/bbstreamer_inject.c:18-27`