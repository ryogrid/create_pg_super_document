# ArchiveHandle

## Location
[src/bin/pg_dump/pg_backup_archiver.h:99-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.h#L99-L99)

## Overview
ArchiveHandle is a typedef for the PostgreSQL dump/restore archive handle structure that provides a unified interface for handling different archive formats during database backup and restore operations.

## Definition

```c
typedef struct _archiveHandle ArchiveHandle;
```
## Detailed Description
ArchiveHandle serves as the primary handle type for PostgreSQL's pg_dump and pg_restore utilities. It is a typedef that points to the internal  structure, which contains all the state information, function pointers, and metadata needed to manage archive operations across different formats (custom, tar, plain text, etc.).

The structure encapsulates both the public interface (through the Archive member) and internal implementation details required for:
- Reading and writing archive data
- Managing table of contents (TOC) entries
- Handling different compression formats
- Supporting parallel dump/restore operations
- Managing database connections during direct mode operations

## Parameters / Member Variables
Since ArchiveHandle is a typedef, it inherits all members from :
- : Public Archive interface
- : Archive format version
- : Version of the dumped database
- : Version of the dumper tool
- : Archive format type (custom, tar, plain, etc.)
- : Circular list of table of contents entries
- : Database connection for direct operations
- : Format-specific data storage
- Function pointers for format-specific operations (WriteDataPtr, ReadDataPtr, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - _archiveHandle (underlying structure)
  - [Archive](Archive.md) (public interface structure)
  - [TocEntry](../T/TocEntry.md) (table of contents entries)
  
- Called from (representative examples):
  - Used throughout pg_dump and pg_restore utilities
  - [Archive](Archive.md) format handlers (custom, tar, plain text)
  - Parallel worker processes

## Notes and Other Information
- This typedef provides abstraction over the internal archive handle implementation
- The actual functionality is implemented in the  structure
- Essential for all archive operations in PostgreSQL's backup and restore system
- Supports multiple archive formats through polymorphic function pointers
- Thread-safe design enables parallel dump and restore operations