# TocEntry

## Location
src/bin/pg_dump/pg_backup_archiver.h: 100 - 102

## Overview
TocEntry is a typedef for the table of contents entry structure that represents individual database objects and their metadata within PostgreSQL dump archives.

## Definition


## Detailed Description
TocEntry serves as the fundamental unit for organizing and managing database objects during dump and restore operations in PostgreSQL. It is a typedef that points to the internal  structure, which contains comprehensive metadata about individual database objects such as tables, indexes, functions, views, and other schema elements.

Each TocEntry represents:
- A single database object or operation within the dump
- Metadata including object name, schema, owner, and dependencies
- Restoration requirements and ordering constraints
- Data location information within the archive
- Access control and security information

The entries are organized in a circular linked list within the archive handle, enabling efficient traversal and dependency resolution during restore operations.

## Parameters / Member Variables
Since TocEntry is a typedef, it inherits all members from :
- Object identification (name, schema, tablespace)
- Dependency information for proper restore ordering
- Archive location data (file offsets, data chunks)
- Restoration metadata (owner, permissions, comments)
- Linked list pointers for navigation
- Parallel processing state information

## Dependencies
- Functions called/Symbols referenced:
  - _tocEntry (underlying structure definition)
  - ParallelState (for parallel processing operations)
  
- Called from (representative examples):
  - RestoreArchive (main restore coordination)
  - ArchiveEntry (entry creation during dump)
  - buildTocEntryArrays (dependency resolution)
  - restore_toc_entry (individual entry restoration)
  - WriteDataChunks (data output operations)
  - parallel restore workers throughout the codebase

## Notes and Other Information
- Essential component of PostgreSQL's dump/restore architecture
- Used extensively in both pg_dump and pg_restore utilities
- Supports parallel processing through dependency tracking
- Enables selective restore operations through filtering mechanisms
- Critical for maintaining referential integrity during restoration
- The actual implementation details are contained in the  structure definition