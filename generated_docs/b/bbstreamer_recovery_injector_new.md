# bbstreamer_recovery_injector_new

## Location
src/bin/pg_basebackup/bbstreamer_inject.c: 65 - 84

## Overview
Creates a bbstreamer that can edit recovery configuration data into an archive stream, enabling modification of recovery settings during base backup operations.

## Definition


## Detailed Description
This function creates a specialized bbstreamer instance that modifies an archive stream to inject recovery configuration data. The function handles different scenarios based on whether recovery GUCs are supported in the PostgreSQL version:

1. **Legacy mode** (is_recovery_guc_supported = false): Places content into recovery.conf, replacing any existing archive member with that name
2. **Modern mode with existing postgresql.auto.conf**: Appends the recovery content to the existing postgresql.auto.conf file in the archive  
3. **Modern mode without postgresql.auto.conf**: Creates a new postgresql.auto.conf file with the specified recovery content

Additionally, when recovery GUCs are supported, it creates a zero-length standby.signal file, removing any existing file with that name from the archive.

The function initializes a bbstreamer_recovery_injector structure with the appropriate operations table and configuration parameters.

## Parameters / Member Variables
- : The next bbstreamer in the processing chain to forward chunks to
- : Boolean flag indicating whether the target PostgreSQL version supports recovery configuration via GUCs rather than recovery.conf
- : PQExpBuffer containing the recovery configuration content to inject into the archive

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - bbstreamer_recovery_injector_ops
  - [bbstreamer_recovery_injector](bbstreamer_recovery_injector.md) (struct type)
  - [bbstreamer_ops](bbstreamer_ops.md) (struct type)
- Called from (representative examples):
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md)
  - [bbstreamer_buffer_until](bbstreamer_buffer_until.md)

## Notes and Other Information
- The input should be a series of typed chunks following bbstreamer.h conventions
- Output chunks maintain the same typing but BBSTREAMER_MEMBER_HEADER chunks may be zero-length when the archive stream has been edited
- Part of PostgreSQL's base backup streaming infrastructure for handling recovery configuration injection
- Located in src/bin/pg_basebackup/bbstreamer_inject.c:65-84