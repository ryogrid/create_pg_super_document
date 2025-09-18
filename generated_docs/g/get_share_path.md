# get_share_path

## Location
[src/port/path.c:901-909](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L901-L909)

## Overview
Retrieves the path to PostgreSQL's shared data directory, with support for relocated installations.

## Definition


## Detailed Description
The  function determines the location of PostgreSQL's shared data directory. This directory typically contains shared files such as extension control files, configuration templates, timezone data, and other read-only data files that are shared across all PostgreSQL installations.

The function is a thin wrapper around , which handles the logic for supporting relocated PostgreSQL installations. It uses the compiled-in values  (target share directory path) and  (compiled-in binary directory path) along with the actual executable location to compute the correct share directory path.

This approach allows PostgreSQL installations to be moved to different locations while maintaining the correct relative paths between the binary directory and the share directory.

## Parameters / Member Variables
- : The full path to the current PostgreSQL executable
- : Output buffer (must be MAXPGPATH size) to store the resulting share directory path

## Dependencies
- Functions called/Symbols referenced:
  -  - Core path relocation logic using PGSHAREDIR, PGBINDIR, and my_exec_path
  -  - Compile-time constant for the share directory path
  -  - Compile-time constant for the binary directory path

- Called from (representative examples):
  -  - Finding extension control files
  -  - Locating specific extension control files
  -  - Finding extension SQL script files
  -  - Text search configuration files
  -  - Timezone data file parsing
  -  - initdb path setup
  -  - Configuration information retrieval
  -  - Timezone directory setup

## Notes and Other Information
- Part of PostgreSQL's installation relocation support system
- Essential for finding shared data files regardless of where PostgreSQL is installed
- Used extensively throughout PostgreSQL for locating configuration files, extensions, and other shared resources
- The function assumes that ret_path buffer has been allocated with at least MAXPGPATH bytes
- Critical for proper operation of extensions, timezone handling, and configuration file processing
- Works in conjunction with other  functions to provide a complete path resolution system