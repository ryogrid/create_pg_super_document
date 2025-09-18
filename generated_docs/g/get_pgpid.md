# get_pgpid

## Location
[src/bin/pg_ctl/pg_ctl.c:245-312](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L245-L312)

## Overview
Reads and validates the PostgreSQL server's process ID from the PID file, with comprehensive error checking for various failure scenarios.

## Definition


## Detailed Description
The  function is a critical component of pg_ctl that retrieves the PostgreSQL server's process ID by reading from the PID file. It performs extensive validation of the data directory and PID file to ensure they exist and contain valid data.

The function first validates that the PostgreSQL data directory exists and is accessible. It then checks for the presence of the version file to confirm it's actually a database cluster directory. Finally, it opens and reads the PID file, parsing the process ID and performing validation on the file contents.

The function handles different error scenarios appropriately, using different exit codes based on whether this is a status request (following Linux Standard Base specifications for service status) or a regular operational request.

## Parameters / Member Variables
- : Boolean flag indicating whether this call is for a status check operation, affects exit codes returned for certain error conditions

## Dependencies
- Functions called/Symbols referenced:
  -  (system call for file/directory status)
  -  (PostgreSQL error output function)
  -  (standard C library)
  -  (standard C library)
  -  (standard C library)  
  -  (standard C library)
  -  (standard C library)
- Called from (representative examples):
  -  (pg_ctl.c:930)
  -  (pg_ctl.c:1019)
  -  (pg_ctl.c:1077)
  -  (pg_ctl.c:1141)
  -  (pg_ctl.c:1179)
  -  (pg_ctl.c:1340)
  -  (pg_ctl.c:717, 726)

## Notes and Other Information
- Returns 0 if PID file doesn't exist (not an error during startup)
- Follows Linux Standard Base Core Specification 3.1 for status request exit codes
- Validates data directory accessibility before attempting to read PID file
- Confirms directory is actually a PostgreSQL cluster by checking for version file
- Handles both empty PID files and files with invalid data
- Critical for all pg_ctl operations that need to interact with running PostgreSQL server
- Uses different exit codes (1 vs 4) depending on whether it's a status request
- Static function, only available within pg_ctl.c