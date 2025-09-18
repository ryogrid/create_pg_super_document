# bbstreamer_extractor_content

## Location
[src/bin/pg_basebackup/bbstreamer_file.c:203-291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_file.c#L203-L291)

## Overview
This function handles the extraction of archive contents to the filesystem, processing different types of archive members (files, directories, symbolic links) and managing the extraction workflow through various archive contexts.

## Definition


## Detailed Description
The  function is the core content processing function for the bbstreamer extractor. It operates as a state machine that handles different phases of archive extraction based on the provided context. The function constructs full file paths by prepending the base path, handles different member types (directories, symbolic links, regular files), and manages file I/O operations during extraction.

For each archive member, the function processes four main contexts:
1. **BBSTREAMER_MEMBER_HEADER**: Initializes extraction for a new member, determines the member type, and creates the appropriate filesystem entity
2. **BBSTREAMER_MEMBER_CONTENTS**: Writes file data to the currently open file
3. **BBSTREAMER_MEMBER_TRAILER**: Finalizes the current member extraction by closing files
4. **BBSTREAMER_ARCHIVE_TRAILER**: Signals the end of the entire archive

## Parameters / Member Variables
- : The bbstreamer instance, cast to bbstreamer_extractor for access to extractor-specific fields
- : Archive member metadata (NULL for archive trailer context)
- : Raw data content to be processed or written to files
- : Length of the data buffer
- : Current phase of archive processing (header, contents, member trailer, or archive trailer)

## Dependencies
- Functions called/Symbols referenced:
  - [extract_directory](../e/extract_directory.md)
  - [extract_link](../e/extract_link.md)  
  - [create_file_for_extract](../c/create_file_for_extract.md)
  - fwrite
  - fclose
  - snprintf
  - strlen
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - Referenced through bbstreamer function pointer mechanism (no direct callers found)

## Notes and Other Information
- This is a static function specific to the bbstreamer file extraction implementation
- The function includes error handling for write failures, assuming ENOSPC (no disk space) when errno is not set
- Link target remapping is supported through the optional link_map callback
- File path construction removes trailing slashes from directory names
- The function reports output file changes through an optional callback mechanism
- Located in src/bin/pg_basebackup/bbstreamer_file.c:203-291