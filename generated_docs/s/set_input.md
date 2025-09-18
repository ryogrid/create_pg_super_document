# set_input

## Location
src/bin/initdb/initdb.c: 979 - 987

## Overview
A static utility function in initdb that constructs full file paths by combining the PostgreSQL share directory path with a given filename.

## Definition


## Detailed Description
The  function is a simple path construction utility used during PostgreSQL database cluster initialization. It takes a filename and creates a complete path by prepending the PostgreSQL share directory path (stored in the global variable ). The function uses  to format the path as "share_path/filename" and assigns the result to the destination pointer. This is primarily used to locate template files and configuration files needed during initdb operations.

## Parameters / Member Variables
- : A pointer to a char pointer where the constructed full path will be stored
- : The filename to be appended to the share directory path

## Dependencies
- Functions called/Symbols referenced:
  - psprintf (PostgreSQL's printf-like memory allocating function)
  - share_path (global variable containing the PostgreSQL share directory path)
- Called from (representative examples):
  - setup_data_file_paths (called multiple times to set up various template file paths)

## Notes and Other Information
- This is a static function, only accessible within initdb.c
- The function assumes that  has been properly initialized before being called
- Memory allocated by  should be managed by the caller
- Used extensively in  function to configure paths for template files like pg_hba.conf, pg_ident.conf, postgresql.conf, etc.
- The share_path typically points to the PostgreSQL installation's share directory containing template and configuration files