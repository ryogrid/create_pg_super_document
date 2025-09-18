# libpq_traverse_files

## Location
src/bin/pg_rewind/libpq_source.c: 233 - 325

## Overview
Recursively traverses the PostgreSQL data directory via a libpq connection to retrieve a complete list of all files and directories, including tablespace symbolic links.

## Definition


## Detailed Description
This function implements the file traversal functionality for the libpq-based rewind source in pg_rewind. It uses a complex recursive SQL query to enumerate all files and directories in the PostgreSQL data directory remotely through a database connection. The query leverages PostgreSQL's built-in functions  and  to build a complete directory tree.

The function handles special cases for tablespaces by joining with the  catalog to resolve tablespace symbolic links. It distinguishes between absolute path tablespaces (true symbolic links) and in-place tablespaces (directories with relative paths) within the  directory.

For each file or directory found, the function determines its type (regular file, directory, or symbolic link) and invokes the provided callback function with the file information.

## Parameters / Member Variables
- : Pointer to the rewind_source structure containing the libpq connection information
- : Function pointer to the callback that will be invoked for each file/directory found during traversal

## Dependencies
- Functions called/Symbols referenced:
  - PQexec (executes the recursive directory listing SQL query)
  - PQresultStatus (checks query execution status)
  - PQresultErrorMessage (retrieves error messages on query failure)
  - PQnfields (validates result set structure)
  - PQntuples (gets number of result rows)
  - PQgetisnull (checks for NULL values in result set)
  - PQgetvalue (retrieves column values from result set)
  - PQclear (frees result set memory)
  - is_absolute_path (determines if tablespace path is absolute)
  - pg_fatal (reports fatal errors)
- Called from:
  - init_libpq_source (as part of libpq_source function table initialization)

## Notes and Other Information
- The function uses a sophisticated WITH RECURSIVE SQL query that first lists the root directory contents, then recursively expands subdirectories
- Special handling is implemented for PostgreSQL tablespaces in the  directory
- Files that are removed during query execution are gracefully ignored (NULL size check)
- The query excludes hidden files and directories via the  parameters
- Custom symbolic links in the data directory are not handled correctly due to lack of a general backend function for retrieving symbolic link targets
- This is a static function used internally within the libpq_source.c module as part of the pg_rewind tool's remote file system access capabilities