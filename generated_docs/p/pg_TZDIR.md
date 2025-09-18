# pg_TZDIR

## Location
src/bin/initdb/findtimezone.c: 37 - 64

## Overview
Returns the full pathname of the timezone data directory for PostgreSQL to use when reading timezone files.

## Definition


## Detailed Description
The pg_TZDIR function is a simple utility that returns the path to the timezone data directory. It provides a layer of abstraction for accessing timezone files, supporting both PostgreSQL's internal timezone database and system-provided timezone data.

The function uses conditional compilation to determine the source of timezone data:
- When SYSTEMTZDIR is not defined (normal case), it returns the value of the global variable , which points to PostgreSQL's own timezone database under the share directory
- When SYSTEMTZDIR is defined at compile time, it returns the system's timezone directory path instead

This design allows PostgreSQL to be configured to use either its bundled timezone database or the system's timezone database, providing flexibility for different deployment scenarios.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - tzdirpath (global variable, when SYSTEMTZDIR not defined)
  - SYSTEMTZDIR (preprocessor macro, when defined)
- Called from (representative examples):
  - [pg_open_tzfile](pg_open_tzfile.md) (in src/bin/initdb/findtimezone.c and src/timezone/pgtz.c)
  - [pg_tzenumerate_start](pg_tzenumerate_start.md) (in src/timezone/pgtz.c)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the file where it's defined
- The function assumes that  has been properly set up by  before being called
- The conditional compilation allows PostgreSQL installations to choose between using PostgreSQL's maintained timezone database or relying on the system's timezone data
- This function is part of PostgreSQL's timezone handling infrastructure and is critical for timezone file operations