# bbstreamer_recovery_injector_content

## Location
src/bin/pg_basebackup/bbstreamer_inject.c: 85 - 199

## Overview
Handles each chunk of tar content while injecting recovery configuration, managing file filtering and content modification based on the archive context.

## Definition


## Detailed Description
This function processes archive stream chunks and selectively modifies them to inject recovery configuration. It operates differently based on the archive context and recovery GUC support:

**BBSTREAMER_MEMBER_HEADER context:**
- Copies member data for potential modification
- For modern PostgreSQL (recovery GUCs supported): skips standby.signal files and modifies postgresql.auto.conf by increasing its size to accommodate injected content
- For legacy PostgreSQL: skips recovery.conf files
- Invalidates archive headers when content will be modified

**BBSTREAMER_MEMBER_CONTENTS and BBSTREAMER_MEMBER_TRAILER contexts:**
- Skips forwarding data for files marked to be skipped
- Appends recovery configuration content to postgresql.auto.conf files during trailer processing

**BBSTREAMER_ARCHIVE_TRAILER context:**
- For modern PostgreSQL: creates postgresql.auto.conf if not found, and injects empty standby.signal file
- For legacy PostgreSQL: injects recovery.conf with specified contents

The function ensures proper recovery configuration injection while maintaining archive stream integrity.

## Parameters / Member Variables
- : The bbstreamer instance (cast to bbstreamer_recovery_injector)
- : Archive member information (NULL for trailer context)
- : Chunk data to process
- : Length of the data chunk
- : Current archive context (header, contents, trailer, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - memcpy
  - strcmp
  - [bbstreamer_content](bbstreamer_content.md)
  - [bbstreamer_inject_file](bbstreamer_inject_file.md)
  - [pg_fatal](../p/pg_fatal.md)
  - bbstreamer_member (struct type)
  - [bbstreamer_archive_context](bbstreamer_archive_context.md) (enum)
  - BBSTREAMER_* constants
- Called from (representative examples):
  - No direct references found (likely called via function pointer in operations table)

## Notes and Other Information
- Static function used as part of the bbstreamer_recovery_injector operations table
- Handles complex logic for different PostgreSQL versions (legacy vs. recovery GUC support)
- Modifies archive headers when injecting content, requiring subsequent bbstreamers to regenerate them
- Critical for maintaining archive integrity while injecting recovery configuration files
- Located in src/bin/pg_basebackup/bbstreamer_inject.c:85-199