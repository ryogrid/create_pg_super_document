# dumpEncoding

## Location
src/bin/pg_dump/pg_dump.c: 3565 - 3589

## Overview
The  function saves the database encoding information to the archive as a SET client_encoding command for proper restoration.

## Definition


## Detailed Description
The  function creates an archive entry that contains a SQL command to set the client encoding to match the database encoding. This ensures that when the dump is restored, the client connection uses the same encoding as the original database. The function converts the numeric encoding stored in the archive to its string representation, wraps it in a SET client_encoding statement, and stores it as a PRE_DATA section entry in the dump archive.

## Parameters / Member Variables
- : Pointer to the Archive structure containing dump state and configuration, including the encoding value

## Dependencies
- Functions called/Symbols referenced:
  - pg_encoding_to_char (converts encoding ID to string name)
  - pg_log_info (logs the encoding being saved)
  - createPQExpBuffer/appendPQExpBufferStr/destroyPQExpBuffer (string buffer management)
  - appendStringLiteralAH (safely quotes the encoding name as SQL literal)
  - createDumpId (generates unique dump ID)
  - ArchiveEntry (creates archive entry with SQL command)
  - ARCHIVE_OPTS/SECTION_PRE_DATA (archive configuration macros)
- Called from (representative examples):
  - main (pg_dump main function)
  - fmtQualifiedDumpable

## Notes and Other Information
- This function is part of the pg_dump utility's core dumping logic
- The encoding entry is placed in SECTION_PRE_DATA to ensure it's executed before other database objects are restored
- The function handles proper SQL literal quoting to prevent injection issues with encoding names
- This is essential for correctly restoring databases with non-ASCII character sets