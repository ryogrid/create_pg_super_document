# getInstallationPaths

## Location
[src/backend/postmaster/postmaster.c:1435-1488](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L1435-L1488)

## Overview
getInstallationPaths determines and validates the filesystem paths to PostgreSQL installation directories based on the location of the postgres executable, ensuring the installation is complete and accessible.

## Definition


## Detailed Description
getInstallationPaths performs critical path discovery and validation during postmaster startup to establish the locations of key PostgreSQL installation components. The function implements a systematic approach to installation path resolution:

1. **Executable Location Discovery**: Uses find_my_exec() to determine the absolute path of the currently running postgres executable from argv[0]. This handles cases where the executable was invoked with a relative path, through a symlink, or via PATH resolution.

2. **Backend Executable Location (EXEC_BACKEND only)**: On platforms requiring EXEC_BACKEND (primarily Windows), locates the matching postgres backend executable and stores its path. This is necessary because child processes are created via fork()/exec() rather than fork() alone.

3. **Package Library Path Resolution**: Calls get_pkglib_path() to compute the location of the pkglib directory (typically lib/postgresql or similar) relative to the executable location. This directory contains loadable modules, extensions, and shared libraries.

4. **Installation Validation**: Opens and immediately closes the pkglib directory to verify it exists and is accessible. This validation catches incomplete installations or cases where the executable has been moved without its supporting files.

The function is designed to fail fast with informative error messages if the PostgreSQL installation appears corrupted or incomplete, preventing later mysterious failures when attempting to load modules or access installation resources.

## Parameters / Member Variables
- : The program name as passed in argv[0] from main(), used to locate the executable's path

## Dependencies
- Functions called/Symbols referenced:
  - find_my_exec: Resolve full path to current executable
  - find_other_exec: Locate matching postgres backend (EXEC_BACKEND builds only)
  - [get_pkglib_path](get_pkglib_path.md): Calculate package library directory path
  - AllocateDir: Open directory for validation
  - FreeDir: Close directory after validation
  - DIR: Directory handle type
  - PG_BACKEND_VERSIONSTR: Version string for backend matching
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md): Called early in startup sequence at line 532
  - Referenced in SIGKILL_CHILDREN_AFTER_SECS context

## Notes and Other Information
- Must be called before attempting to load any shared modules or extensions
- Sets global variables my_exec_path, postgres_exec_path (EXEC_BACKEND), and pkglib_path
- The validation is conservative - share/ directory checking is skipped on the assumption that if lib/ exists, share/ likely does too
- Common failure scenarios include hardlinking or copying postgres executable to a different location without the supporting directory structure
- Critical for proper operation of shared_preload_libraries and dynamic module loading
- The pkglib_path discovery supports both standard installations and custom/relocatable installations