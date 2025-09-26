# bbstreamer_inject_file

## Location
[src/bin/pg_basebackup/bbstreamer_inject.c:219-249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_inject.c#L219-L249)

## Overview
Injects a complete file into the archive stream with specified contents, creating a new archive member with appropriate metadata.

## Definition

```c
void
bbstreamer_inject_file(bbstreamer *streamer, char *pathname, char *data,
					   int len)
```
## Detailed Description
This utility function creates and injects a complete file into the bbstreamer pipeline by constructing a bbstreamer_member structure with appropriate metadata and sending the file through the standard three-phase archive member protocol (header, contents, trailer).

The function sets up the archive member with:
- File path and size based on provided parameters
- Standard file mode (pg_file_create_mode)
- Historical UID/GID values (04000/02000) used by PostgreSQL
- Proper file type flags (not directory, not link)

The function generates three consecutive bbstreamer_content calls to properly represent the archive member structure, allowing downstream bbstreamers to handle archive format-specific header and trailer generation as needed.

## Parameters / Member Variables
- : The bbstreamer to send the injected file to
- : File path for the injected archive member (copied up to MAXPGPATH)
- : File content data to inject
- : Length of the data to inject

## Dependencies
- Functions called/Symbols referenced:
  - [strlcpy](../s/strlcpy.md)
  - [bbstreamer_content](bbstreamer_content.md)
  - bbstreamer_member (struct type)
  - pg_file_create_mode (constant)
  - MAXPGPATH (constant)
  - BBSTREAMER_MEMBER_* constants
- Called from (representative examples):
  - [bbstreamer_recovery_injector_content](bbstreamer_recovery_injector_content.md)
  - [ReceiveArchiveStream](../R/ReceiveArchiveStream.md)
  - [ReceiveTarFile](../R/ReceiveTarFile.md)

## Notes and Other Information
- Public function used by multiple components in pg_basebackup
- Creates archive members with historical PostgreSQL UID/GID values (04000/02000)
- Delegates archive format-specific header/trailer generation to successor bbstreamers
- Essential for injecting recovery configuration files and standby.signal
- Part of the base backup streaming infrastructure
- Located in src/bin/pg_basebackup/bbstreamer_inject.c:219-249