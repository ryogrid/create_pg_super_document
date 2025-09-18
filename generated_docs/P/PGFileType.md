# PGFileType

## Location
[src/include/common/file_utils.h:25-26](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/file_utils.h#L25-L26)

## Overview
PGFileType is an enumeration that defines different types of file system objects that PostgreSQL can encounter when working with directories and files.

## Definition


## Detailed Description
The PGFileType enumeration is used throughout PostgreSQL to classify file system objects into distinct categories. This classification is essential for proper file handling operations such as directory scanning, backup operations, and file system synchronization. The enum provides a standardized way to represent file types across different platforms and file system operations.

## Parameters / Member Variables
- : Indicates an error occurred while determining the file type
- : File type could not be determined or is not recognized
- : Regular file (standard data file)
- : Directory
- : Symbolic link

## Dependencies
- Functions called/Symbols referenced: None (enum definition)
- Called from (representative examples):
  -  at src/common/file_utils.c:530
  -  at src/backend/storage/file/copydir.c:53
  -  at src/backend/storage/file/fd.c:3355
  -  at src/backend/access/heap/rewriteheap.c:1186
  -  at src/backend/access/transam/xlog.c:9000
  -  at src/bin/pg_combinebackup/pg_combinebackup.c:936

## Notes and Other Information
This enumeration is defined in src/include/common/file_utils.h and is used primarily by the get_dirent_type() function to classify directory entries. It plays a crucial role in PostgreSQL's file system operations, particularly in backup and recovery operations, directory synchronization, and temporary file cleanup. The enum values are designed to handle cross-platform file system differences and provide consistent file type identification across different operating systems.