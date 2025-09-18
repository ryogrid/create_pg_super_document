# cleanup

## Location
src/backend/regex/regc_nfa.c: 2964 - 2998

## Overview
The cleanup function is a static utility function in the PostgreSQL bootstrap module that performs cleanup operations by closing any open relation descriptor if it exists.

## Definition


## Detailed Description
The cleanup function serves as a simple cleanup routine specifically designed for the bootstrap mode of PostgreSQL. It checks if there's an active relation descriptor (boot_reldesc) and if so, calls closerel() to properly close it. This function ensures that any relation opened during bootstrap operations is properly closed, preventing resource leaks and maintaining system consistency.

The function is part of the bootstrap subsystem which is responsible for initializing the PostgreSQL database system during the initial database creation process.

## Parameters / Member Variables
- None (void function with no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - closerel
- Called from (representative examples):
  - BootstrapModeMain (src/backend/bootstrap/bootstrap.c:367)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the bootstrap.c file
- The function relies on the global variable boot_reldesc to determine if cleanup is necessary
- It's specifically used during bootstrap mode operations to ensure proper resource cleanup
- The function is very simple but critical for preventing resource leaks during database initialization