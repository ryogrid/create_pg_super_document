# get_man_path

## Location
src/port/path.c: 991 - 1003

## Overview
Constructs the full path to PostgreSQL's manual pages directory by calculating a relative path from the executable location.

## Definition


## Detailed Description
This function determines the absolute path to PostgreSQL's manual pages (man pages) directory based on the location of the current executable. It uses the  function to compute the path by using the compile-time constants MANDIR and PGBINDIR to establish the relative relationship between the binary directory and the manual pages directory.

The function enables PostgreSQL utilities and applications to locate Unix manual page files at runtime, supporting command-line help systems in relocatable installations where the actual installation path may differ from the compile-time configured paths.

## Parameters / Member Variables
- : Input parameter containing the full path to the current executable
- : Output buffer where the computed manual pages directory path will be stored (must be at least MAXPGPATH bytes)

## Dependencies
- Functions called/Symbols referenced:
  - make_relative_path
  - MANDIR (compile-time constant)
  - PGBINDIR (compile-time constant)
- Called from (representative examples):
  - get_configdata (src/common/config_info.c:102)

## Notes and Other Information
- Part of PostgreSQL's installation path resolution system for documentation
- Specifically targets Unix manual page files (typically in sections 1, 3, 7, etc.)
- The function assumes ret_path buffer is sufficiently large (MAXPGPATH)
- Used by configuration reporting tools to locate manual page files
- Essential for Unix/Linux environments where man pages provide command-line documentation
- Supports relocatable installations where documentation paths need dynamic resolution
- Complements other documentation path functions (get_doc_path, get_html_path)