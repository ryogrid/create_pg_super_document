# bbstreamer_archive_context

## Location
src/bin/pg_basebackup/bbstreamer.h: 60 - 79

## Overview
An enumeration that defines the classification of data chunks within archive processing streams in PostgreSQL's base backup functionality.

## Definition


## Detailed Description
The  enum is a fundamental component of PostgreSQL's base backup streaming architecture, specifically designed for parsing and processing archive formats like tar. It provides a structured way to classify different types of data chunks as they flow through the bbstreamer pipeline.

When archives are parsed (using components like ), this enum enables precise classification of data chunks according to their role within the archive structure. Each chunk must be labeled with one of these context types, allowing downstream processors to handle the data appropriately.

The enum enforces a strict structural contract: each archive member must have exactly one  and one  chunk, even if zero-length. Between these, any number of  chunks can exist. The entire archive concludes with exactly one  chunk.

## Parameters / Member Variables
- : Indicates unparsed or unclassified data chunks that require further processing
- : Identifies chunks containing archive member header information (metadata like filename, size, permissions)
- : Marks chunks containing the actual file data/content of an archive member
- : Designates chunks containing trailing data for an archive member (typically padding bytes)
- : Identifies the final chunk that marks the end of the entire archive

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a standalone enum)
- Called from (representative examples):
  -  (src/bin/pg_basebackup/bbstreamer.h:128)
  -  (src/bin/pg_basebackup/bbstreamer_tar.c:113)
  -  (src/bin/pg_basebackup/bbstreamer_file.c:106)
  -  (src/bin/pg_basebackup/bbstreamer_file.c:205)
  - Various compression/decompression bbstreamer implementations

## Notes and Other Information
- This enum is central to the bbstreamer architecture and must be used consistently across all bbstreamer implementations
- The  context is typically used for raw, unparsed input that needs to be processed by a parser (like tar parser)
- Proper sequencing of contexts is critical - violating the expected order can lead to parsing errors or data corruption
- The enum supports extensibility - additional archive formats could potentially add new context types, though currently it's optimized for tar format
- All bbstreamer content handlers receive this context parameter to determine how to process the associated data chunk
- Used extensively in pg_basebackup utility for creating and extracting database backup archives