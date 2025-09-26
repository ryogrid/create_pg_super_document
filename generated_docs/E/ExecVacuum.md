# ExecVacuum

## Location
src/backend/commands/vacuum.c: 148 - 478

## Overview
Primary entry point for manual VACUUM and ANALYZE commands, serving as a preparation wrapper that parses options and delegates to the vacuum() function.

## Definition


## Detailed Description
ExecVacuum is the main preparation and coordination function for user-initiated VACUUM and ANALYZE commands. It parses the SQL statement's options, validates parameters, constructs the VacuumParams structure, and creates necessary memory contexts and buffer strategies before calling the core vacuum() function.

The function handles extensive option parsing including verbose mode, skip_locked, buffer usage limits, parallel processing, index cleanup strategies, and various specialized vacuum modes. It performs comprehensive validation of option combinations, ensuring incompatible options are rejected with appropriate error messages.

Key responsibilities include:
- Parsing and validating all VACUUM/ANALYZE options from the SQL statement
- Setting up VacuumParams structure with appropriate flags and values
- Creating a cross-transaction memory context for vacuum operations
- Establishing buffer access strategies for efficient I/O management
- Enforcing business rules and option compatibility constraints
- Delegating actual vacuum work to the vacuum() function

## Parameters / Member Variables
- : ParseState containing parser context information for error reporting
- : VacuumStmt structure containing the parsed VACUUM/ANALYZE statement with options and target relations
- : Boolean indicating whether this is a top-level command (affects transaction handling)

## Dependencies
- Functions called/Symbols referenced:
  - vacuum (core vacuum implementation)
  - defGetBoolean, defGetString, defGetInt32 (option parsing utilities)
  - parse_int (string to integer conversion with units)
  - get_vacoptval_from_boolean (option value conversion)
  - AllocSetContextCreate (memory context creation)
  - GetAccessStrategyWithSize (buffer strategy creation)
  - MemoryContextDelete (cleanup)
- Called from (representative examples):
  - standard_ProcessUtility (main utility command dispatcher)

## Notes and Other Information
- Supports extensive option validation including buffer usage limits, parallel worker counts, and option compatibility
- Creates a special "Vacuum" memory context as a child of PortalContext for cross-transaction storage
- Handles both VACUUM and ANALYZE operations through unified option processing
- Enforces numerous business rules: VACUUM FULL cannot be parallelized, BUFFER_USAGE_LIMIT incompatible with VACUUM FULL (except when combined with ANALYZE)
- Buffer usage limits are validated against MIN_BAS_VAC_RING_SIZE_KB and MAX_BAS_VAC_RING_SIZE_KB constants
- Supports specialized database-only statistics operations through ONLY_DATABASE_STATS option
- Default parallel vacuum is enabled (nworkers = 0 means auto-detect optimal worker count)