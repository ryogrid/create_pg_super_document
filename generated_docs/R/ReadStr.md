# ReadStr

## Location
src/bin/pg_dump/pg_backup_archiver.c: 2189 - 2208

## Overview
ReadStr is a function in PostgreSQL's pg_dump archiver that reads a null-terminated string from an archive handle.

## Definition


## Detailed Description
ReadStr reads a string from a PostgreSQL archive by first reading an integer representing the string length, then allocating memory and reading the string data. The function handles both null strings (when length is negative) and normal strings. It ensures proper null termination of the read string and returns a dynamically allocated buffer that the caller must eventually free.

## Parameters / Member Variables
- : ArchiveHandle pointer - the archive handle containing the data to read from

## Dependencies
- Functions called/Symbols referenced:
  - [ReadInt](ReadInt.md)
  - pg_malloc
- Called from (representative examples):
  - [ReadToc](ReadToc.md)
  - [ReadHead](ReadHead.md)
  - appendByteaLiteralAHX
  - [_ReadExtraToc](_ReadExtraToc.md)

## Notes and Other Information
- Returns NULL when the stored length is negative (indicating a null string)
- The caller is responsible for freeing the returned memory using pg_free
- Used extensively in TOC (Table of Contents) reading operations for archive restoration
- Part of the archive format abstraction layer in pg_dump/pg_restore utilities