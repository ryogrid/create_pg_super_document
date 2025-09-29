# GetFdwRoutineByServerId

## Location
[src/backend/foreign/foreign.c:377-418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/foreign/foreign.c#L377-L418)

## Overview
Retrieves the FdwRoutine structure for a foreign data wrapper by looking up the handler function associated with a given foreign server ID.

## Definition
```c
FdwRoutine *GetFdwRoutineByServerId(Oid serverid)
```

## Detailed Description
This function performs a two-step lookup process to obtain the FdwRoutine structure for a foreign data wrapper. First, it looks up the foreign server in `pg_foreign_server` to get the foreign data wrapper OID. Then it looks up the foreign data wrapper in `pg_foreign_data_wrapper` to get the handler function OID. Finally, it calls the handler function via `GetFdwRoutine` to obtain the FdwRoutine structure containing all the callback functions for the FDW.

The function includes comprehensive error handling for missing servers, missing FDWs, and FDWs configured without handlers (NO HANDLER option).

## Parameters / Member Variables
- `serverid`: The OID of the foreign server for which to retrieve the FDW routine structure

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - OidIsValid
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - NameStr
  - [GetFdwRoutine](GetFdwRoutine.md)
  - Form_pg_foreign_data_wrapper
  - Form_pg_foreign_server
- Called from (representative examples):
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md)
  - [truncate_check_rel](../t/truncate_check_rel.md)
  - [ExecInitForeignScan](../E/ExecInitForeignScan.md)
  - [GetFdwRoutineByRelId](GetFdwRoutineByRelId.md)

## Notes and Other Information
- Performs cascading catalog lookups: server → FDW → handler function
- Returns a pointer to FdwRoutine structure containing FDW callback functions
- Throws ERROR if server lookup fails or if FDW has no handler configured
- Uses system cache for performance optimization
- Essential for initializing foreign scans and other FDW operations

## Simplified Source

```c
FdwRoutine *GetFdwRoutineByServerId(Oid serverid) {
    HeapTuple tp;
    Form_pg_foreign_data_wrapper fdwform;
    Form_pg_foreign_server serverform;
    Oid fdwid;
    Oid fdwhandler;

    // Look up foreign server to get FDW ID
    tp = SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(serverid));
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for foreign server %u", serverid);

    serverform = (Form_pg_foreign_server) GETSTRUCT(tp);
    fdwid = serverform->srvfdw;
    ReleaseSysCache(tp);

    // Look up FDW to get handler function ID
    tp = SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(fdwid));
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for foreign-data wrapper %u", fdwid);

    fdwform = (Form_pg_foreign_data_wrapper) GETSTRUCT(tp);
    fdwhandler = fdwform->fdwhandler;

    // Check that FDW has a handler function
    if (!OidIsValid(fdwhandler))
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("foreign-data wrapper \"%s\" has no handler",
                        NameStr(fdwform->fdwname))));

    ReleaseSysCache(tp);

    // Call the handler function to get FdwRoutine structure
    return GetFdwRoutine(fdwhandler);
}
```