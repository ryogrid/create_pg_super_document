# GetUserMapping

## Location
[src/backend/foreign/foreign.c:200-253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/foreign/foreign.c#L200-L253)

## Overview
Retrieves a user mapping for a specific user and foreign server, with fallback to PUBLIC mappings if user-specific mapping is not found.

## Definition
```c
UserMapping *GetUserMapping(Oid userid, Oid serverid)
```

## Detailed Description
GetUserMapping is a core function in PostgreSQL's foreign data wrapper system that locates and returns user mapping information for connecting to foreign servers. The function implements a two-tier lookup strategy: first searching for a mapping specific to the given user ID, and if not found, falling back to a PUBLIC mapping (userid == InvalidOid). If neither mapping exists, it raises an error with a descriptive message. The function constructs a complete UserMapping structure containing the mapping ID, user ID, server ID, and processed options extracted from the system catalog.

## Parameters / Member Variables
- `userid`: Object ID of the user for whom to find the mapping
- `serverid`: Object ID of the foreign server

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [GetForeignServer](GetForeignServer.md)
  - MappingUserName
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [untransformRelOptions](../u/untransformRelOptions.md)
  - [palloc](../p/palloc.md)
  - ereport/errmsg/errcode
- Called from (representative examples):
  - Foreign data wrapper connection establishment routines
  - User mapping validation functions

## Notes and Other Information
The function uses the system cache USERMAPPINGUSERSERVER for efficient lookup. The PUBLIC mapping fallback mechanism allows administrators to define default connection parameters for all users of a foreign server. Options are stored in a transformed format and need to be untransformed using untransformRelOptions() before use. The function is located in src/backend/foreign/foreign.c:200-253 and is essential for FDW authentication and connection parameter resolution.

## Simplified Source

```c
UserMapping *
GetUserMapping(Oid userid, Oid serverid)
{
    HeapTuple tp;
    UserMapping *um;

    // Try to find user-specific mapping first
    tp = SearchSysCache2(USERMAPPINGUSERSERVER,
                         ObjectIdGetDatum(userid),
                         ObjectIdGetDatum(serverid));

    // If not found, try PUBLIC mapping
    if (!HeapTupleIsValid(tp)) {
        tp = SearchSysCache2(USERMAPPINGUSERSERVER,
                             ObjectIdGetDatum(InvalidOid),
                             ObjectIdGetDatum(serverid));
    }

    // Error if no mapping found
    if (!HeapTupleIsValid(tp)) {
        ForeignServer *server = GetForeignServer(serverid);
        ereport(ERROR,
                (errmsg("user mapping not found for user \"%s\", server \"%s\"",
                        MappingUserName(userid), server->servername)));
    }

    // Build UserMapping structure
    um = (UserMapping *) palloc(sizeof(UserMapping));
    um->umid = ((Form_pg_user_mapping) GETSTRUCT(tp))->oid;
    um->userid = userid;
    um->serverid = serverid;

    // Extract and parse options
    Datum datum = SysCacheGetAttr(USERMAPPINGUSERSERVER, tp,
                                  Anum_pg_user_mapping_umoptions, &isnull);
    um->options = isnull ? NIL : untransformRelOptions(datum);

    ReleaseSysCache(tp);
    return um;
}
```