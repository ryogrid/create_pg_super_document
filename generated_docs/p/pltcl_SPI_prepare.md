# pltcl_SPI_prepare

## Location
[src/pl/tcl/pltcl.c:2547-2674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L2547-L2674)

## Overview
Implements the built-in SPI_prepare Tcl command for PL/Tcl, allowing preparation and permanent storage of SQL execution plans with parameter type information for later reuse.

## Definition
```c
static int pltcl_SPI_prepare(ClientData cdata, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[])
```

## Detailed Description
This function provides the SPI_prepare functionality as a Tcl command within PL/Tcl procedures. It takes an SQL query string and a list of parameter types, prepares the query plan using PostgreSQL's SPI interface, and stores it permanently for later execution. The function creates a dedicated memory context for the plan, resolves parameter types, prepares the plan within a subtransaction for error safety, and registers the plan in a hash table for future access.

The function uses the subtransaction pattern (pltcl_subtrans_begin/commit/abort) to ensure that preparation errors are handled gracefully without affecting the outer transaction. All plans are automatically saved using SPI_keepplan to ensure they persist beyond the current SPI context.

## Parameters / Member Variables
- `cdata`: Client data passed by Tcl command registration (unused)
- `interp`: The Tcl interpreter where the command is being executed
- `objc`: Number of command arguments (must be 3)
- `objv`: Array of Tcl objects representing command arguments [command, query, argtypes]

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [parseTypeString](parseTypeString.md)
  - [getTypeInputInfo](../g/getTypeInputInfo.md)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [SPI_prepare](../S/SPI_prepare.md)
  - [SPI_keepplan](../S/SPI_keepplan.md)
  - [pltcl_subtrans_begin](pltcl_subtrans_begin.md)
  - [pltcl_subtrans_commit](pltcl_subtrans_commit.md)
  - [pltcl_subtrans_abort](pltcl_subtrans_abort.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - Tcl_WrongNumArgs
  - Tcl_ListObjGetElements
  - Tcl_CreateHashEntry
  - Tcl_SetHashValue
  - Tcl_SetObjResult
  - Tcl_NewStringObj
- Called from (representative examples):
  - [pltcl_init_interp](pltcl_init_interp.md) (command registration)
  - Tcl scripts using SPI_prepare command

## Notes and Other Information
- Expects exactly 3 arguments: command name, SQL query string, and list of argument types
- Creates a dedicated memory context "PL/Tcl spi_prepare query" for plan storage
- Returns a unique plan identifier (query name) that can be used with SPI_execute_plan
- Always uses SPI_keepplan to ensure plans survive beyond the current execution
- Uses subtransaction protection to handle preparation errors safely
- Stores type conversion information (input functions, I/O parameters) for later parameter binding
- [Plan](../P/Plan.md) identifiers are stored in the interpreter's query hash table
- Located in src/pl/tcl/pltcl.c:2547-2674
- Memory leaks can occur if functions are recompiled (noted as FIXME in source)