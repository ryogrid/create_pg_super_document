# pltcl_init_interp

## Location
src/pl/tcl/pltcl.c: 490 - 562

## Overview
Initializes a new Tcl interpreter for PostgreSQL's PL/Tcl procedural language, setting up all necessary commands and infrastructure for SQL integration.

## Definition


## Detailed Description
The `pltcl_init_interp` function creates and configures a new Tcl interpreter as a subsidiary to the main pltcl_hold_interp. This function is responsible for setting up a complete PL/Tcl execution environment that includes:

1. Creating a subsidiary Tcl interpreter with appropriate trust level
2. Initializing the query hash table for prepared statements
3. Installing all PostgreSQL-specific Tcl commands for database interaction
4. Calling the appropriate start procedure if one exists

The function handles both trusted and untrusted interpreter modes, with trusted interpreters having restricted capabilities for security. The interpreter is configured with a comprehensive set of commands that allow Tcl code to interact with PostgreSQL's SPI (Server Programming Interface).

## Parameters / Member Variables
- `interp_desc`: Pointer to the interpreter descriptor structure that will hold the new interpreter
- `prolang`: OID of the procedural language (pltcl or pltclu)
- `pltrusted`: Boolean flag indicating whether to create a trusted (restricted) or untrusted interpreter

## Dependencies
- Functions called/Symbols referenced:
  - `Tcl_CreateSlave` (creates subsidiary interpreter)
  - `Tcl_InitHashTable` (initializes query hash table)
  - `Tcl_CreateObjCommand` (registers PostgreSQL-specific commands)
  - `call_pltcl_start_proc` (calls language start procedure)
  - `Tcl_DeleteInterp` (cleanup on error)
  - Various pltcl command functions (pltcl_elog, pltcl_quote, etc.)
- Called from (representative examples):
  - `pltcl_fetch_interp` (when a new interpreter is needed)

## Notes and Other Information
- Uses PostgreSQL's exception handling (PG_TRY/PG_CATCH) to ensure proper cleanup
- The interpreter name follows the pattern "subsidiary_{user_id}" for identification
- Installs comprehensive command set: elog, quote, argisnull, return_null, return_next, spi_exec, spi_prepare, spi_execp, subtransaction, commit, rollback
- If start procedure fails, the interpreter is properly cleaned up and the exception is re-thrown
- The function is static, indicating it's only used within the pltcl.c module