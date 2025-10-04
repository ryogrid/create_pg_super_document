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

## Simplified Source

```c
static int pltcl_SPI_prepare(ClientData cdata, Tcl_Interp *interp,
                            int objc, Tcl_Obj *const objv[]) {
    MemoryContext plan_cxt = NULL;
    Tcl_Size nargs;
    Tcl_Obj **argsObj;
    pltcl_query_desc *qdesc;
    int i;
    Tcl_HashEntry *hashent;
    int hashnew;
    Tcl_HashTable *query_hash;
    MemoryContext oldcontext = CurrentMemoryContext;
    ResourceOwner oldowner = CurrentResourceOwner;

    // Check syntax: expect "command query argtypes"
    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "query argtypes");
        return TCL_ERROR;
    }

    // Parse argument type list
    if (Tcl_ListObjGetElements(interp, objv[2], &nargs, &argsObj) != TCL_OK) {
        return TCL_ERROR;
    }

    // Create memory context and query descriptor
    plan_cxt = AllocSetContextCreate(TopMemoryContext,
                                    "PL/Tcl spi_prepare query",
                                    ALLOCSET_SMALL_SIZES);
    MemoryContextSwitchTo(plan_cxt);
    qdesc = (pltcl_query_desc *) palloc0(sizeof(pltcl_query_desc));
    snprintf(qdesc->qname, sizeof(qdesc->qname), "%p", qdesc);
    qdesc->nargs = nargs;
    qdesc->argtypes = (Oid *) palloc(nargs * sizeof(Oid));
    qdesc->arginfuncs = (FmgrInfo *) palloc(nargs * sizeof(FmgrInfo));
    qdesc->argtypioparams = (Oid *) palloc(nargs * sizeof(Oid));
    MemoryContextSwitchTo(oldcontext);

    // Execute prepare in subtransaction for error safety
    pltcl_subtrans_begin(oldcontext, oldowner);

    PG_TRY();
    {
        // Resolve parameter types and input functions
        for (i = 0; i < nargs; i++) {
            Oid typId, typInput, typIOParam;
            int32 typmod;

            parseTypeString(Tcl_GetString(argsObj[i]), &typId, &typmod, NULL);
            getTypeInputInfo(typId, &typInput, &typIOParam);

            qdesc->argtypes[i] = typId;
            fmgr_info_cxt(typInput, &(qdesc->arginfuncs[i]), plan_cxt);
            qdesc->argtypioparams[i] = typIOParam;
        }

        // Prepare and keep the plan
        UTF_BEGIN;
        qdesc->plan = SPI_prepare(UTF_U2E(Tcl_GetString(objv[1])),
                                 nargs, qdesc->argtypes);
        UTF_END;

        if (qdesc->plan == NULL) {
            elog(ERROR, "SPI_prepare() failed");
        }

        if (SPI_keepplan(qdesc->plan)) {
            elog(ERROR, "SPI_keepplan() failed");
        }

        pltcl_subtrans_commit(oldcontext, oldowner);
    }
    PG_CATCH();
    {
        pltcl_subtrans_abort(interp, oldcontext, oldowner);
        MemoryContextDelete(plan_cxt);
        return TCL_ERROR;
    }
    PG_END_TRY();

    // Store plan in hash table and return plan name
    query_hash = &pltcl_current_call_state->prodesc->interp_desc->query_hash;
    hashent = Tcl_CreateHashEntry(query_hash, qdesc->qname, &hashnew);
    Tcl_SetHashValue(hashent, (ClientData) qdesc);

    Tcl_SetObjResult(interp, Tcl_NewStringObj(qdesc->qname, -1));
    return TCL_OK;
}
```