# convert_and_check_filename

## Location
[src/backend/utils/adt/genfile.c:54-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L54-L102)

## Overview
Converts a text filename argument to a C string and validates it's allowable for file access based on security policies and user privileges.

## Definition
static char *convert_and_check_filename(text *arg)

## Detailed Description
This function serves as a security gateway for file access operations in PostgreSQL. It converts a PostgreSQL text datum to a C string filename and performs comprehensive security checks to ensure the user has appropriate privileges to access the file. The function implements a dual-tier permission system: superusers with 'pg_read_server_files' role privileges can access any files, while regular users are restricted to files within the DataDir or Log_directory hierarchies. The function canonicalizes the path to resolve any relative components and symbolic links before performing security validation.

## Parameters / Member Variables
- : PostgreSQL text datum containing the filename path to be validated

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring: Converts PostgreSQL text to C string
  - [canonicalize_path](canonicalize_path.md): Normalizes path by resolving relative components
  - has_privs_of_role: Checks if user has privileges of specified role
  - [GetUserId](../G/GetUserId.md): Gets current user ID
  - is_absolute_path: Determines if path is absolute
  - [path_is_prefix_of_path](../p/path_is_prefix_of_path.md): Checks if one path is a prefix of another
  - [path_is_relative_and_below_cwd](../p/path_is_relative_and_below_cwd.md): Validates relative path stays within current directory
  - ereport: Reports errors with specified severity level
- Called from (representative examples):
  - [pg_read_file_common](../p/pg_read_file_common.md): For text file reading operations
  - [pg_read_binary_file_common](../p/pg_read_binary_file_common.md): For binary file reading operations
  - [pg_stat_file](../p/pg_stat_file.md): For file status operations
  - [pg_ls_dir](../p/pg_ls_dir.md): For directory listing operations

## Notes and Other Information
- This function is designed specifically for 'read' access checks and should not be used for 'write' or 'program' access without modifications
- Users with 'pg_read_server_files' role bypass all path restrictions
- Regular users can only access files within DataDir or Log_directory (even if Log_directory is outside DataDir)
- The function throws ERRCODE_INSUFFICIENT_PRIVILEGE errors for unauthorized access attempts
- [Path](../P/Path.md) canonicalization can change the filename length, so the function handles dynamic memory appropriately