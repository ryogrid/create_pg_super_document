# GuessControlValues

## Location
[src/bin/pg_resetwal/pg_resetwal.c:633-715](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_resetwal/pg_resetwal.c#L633-L715)

## Overview
GuessControlValues creates a completely default set of pg_control values when the original control file cannot be read or is corrupted beyond repair.

## Definition


## Detailed Description
This function is called when pg_resetwal cannot read a valid pg_control file and needs to construct reasonable default values from scratch. It initializes all critical PostgreSQL control file fields with safe, conservative defaults that will allow the database to start and operate.

The function performs several key tasks:
1. **System Identifier Generation**: Creates a new unique system identifier using current time and process ID to ensure uniqueness
2. **Checkpoint Initialization**: Sets up initial checkpoint data with safe starting values for transaction IDs, timeline IDs, and LSN positions
3. **Database State Setup**: Configures the database as cleanly shut down with minimal WAL level
4. **Configuration Defaults**: Applies conservative defaults for connection limits, WAL settings, and other operational parameters
5. **Architecture Constants**: Sets platform-specific values like alignment, block sizes, and data type formats

The function sets the global  flag to indicate that all values are estimates rather than actual historical data from a valid control file.

## Parameters / Member Variables
This function takes no parameters and returns void.

## Dependencies
- Functions called/Symbols referenced:
  - memset (memory initialization)
  - gettimeofday (system time retrieval)
  - getpid (process ID retrieval)
  - time (current time)
  - FullTransactionIdFromEpochAndXid (transaction ID construction)
  - Various PostgreSQL constants:
    - PG_CONTROL_VERSION, CATALOG_VERSION_NO
    - SizeOfXLogLongPHD, FirstNormalTransactionId
    - FirstGenbkiObjectId, FirstMultiXactId
    - InvalidOid, InvalidTransactionId
    - DB_SHUTDOWNED, WAL_LEVEL_MINIMAL
    - Architecture constants (MAXIMUM_ALIGNOF, BLCKSZ, etc.)
- Called from:
  - [main](../m/main.md) (in pg_resetwal.c:384)

## Notes and Other Information
- This is a static function, accessible only within pg_resetwal.c
- Sets the global  variable to true to indicate all values are estimates
- Creates a new unique system identifier, making old XLOG records incompatible
- Uses conservative defaults for all configuration parameters to ensure safe operation
- The function includes a TODO comment suggesting future enhancement to analyze old XLOG files for more accurate values
- All checkpoint-related values are set to safe initial states (timeline 1, minimal transaction IDs)
- Database state is set to DB_SHUTDOWNED to indicate a clean startup condition
- Architecture-specific constants are set based on compile-time values to match the current PostgreSQL build
- This function is essentially PostgreSQL's "factory reset" for the control file when all else fails