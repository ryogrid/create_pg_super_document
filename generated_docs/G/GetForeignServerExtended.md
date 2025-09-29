# GetForeignServerExtended

## Location
[src/backend/foreign/foreign.c:123-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/foreign/foreign.c#L123-L181)

## Overview
Retrieves a foreign server object by its Object ID (OID) with extended options for error handling, allowing callers to specify whether missing servers should raise an error or return NULL.

## Definition
```c
ForeignServer *GetForeignServerExtended(Oid serverid, bits16 flags)
```

## Detailed Description
GetForeignServerExtended is the core function for looking up foreign server objects in PostgreSQL's system catalogs. It searches the pg_foreign_server system catalog by OID and constructs a ForeignServer structure containing all the server's metadata. The function supports flexible error handling through the flags parameter - when FSV_MISSING_OK is specified, it returns NULL for non-existent servers instead of raising an error. The function allocates memory for the returned structure and extracts all relevant information including the server's name, owner, associated foreign-data wrapper ID, server type, version, and connection options. Foreign servers represent the configuration for connecting to external data sources through foreign-data wrappers.

## Parameters / Member Variables
- `serverid`: The Object ID (OID) of the foreign server to retrieve
- `flags`: Control flags (bits16) - when FSV_MISSING_OK is set, returns NULL instead of error for missing servers

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (tuple structure extraction)
  - [palloc](../p/palloc.md) (memory allocation)
  - [pstrdup](../p/pstrdup.md) (string duplication)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md) (attribute extraction)
  - TextDatumGetCString (text conversion)
  - [untransformRelOptions](../u/untransformRelOptions.md) (options parsing)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_foreign_server (catalog form structure)
  - FSV_MISSING_OK (flag constant)
- Called from (representative examples):
  - [GetForeignServer](GetForeignServer.md)
  - [getObjectDescription](../g/getObjectDescription.md)
  - [getObjectIdentityParts](../g/getObjectIdentityParts.md)

## Notes and Other Information
- Located in src/backend/foreign/foreign.c:123-181
- Returns a palloc'd ForeignServer structure that must be freed by the caller
- Uses the system cache (FOREIGNSERVEROID) for efficient lookups
- Extracts and handles optional attributes (servertype, serverversion) gracefully when they are NULL
- Extracts and parses srvoptions from the catalog tuple using untransformRelOptions
- The returned structure includes: serverid, servername, owner, fdwid, servertype, serverversion, and options
- Error handling is controlled by the FSV_MISSING_OK flag in the flags parameter
- This is the primary implementation function that other foreign server lookup functions delegate to
- Foreign servers define connection parameters and metadata for accessing external data sources through FDWs

## Simplified Source

```c
ForeignServer *
GetForeignServerExtended(Oid serverid, bits16 flags)
{
    // Look up foreign server in system catalog
    HeapTuple tp = SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(serverid));

    // Handle missing server based on flags
    if (!HeapTupleIsValid(tp)) {
        if ((flags & FSV_MISSING_OK) == 0)
            elog(ERROR, "cache lookup failed for foreign server %u", serverid);
        return NULL;
    }

    // Extract server information from catalog tuple
    Form_pg_foreign_server serverform = (Form_pg_foreign_server) GETSTRUCT(tp);

    // Allocate and populate ForeignServer structure
    ForeignServer *server = (ForeignServer *) palloc(sizeof(ForeignServer));
    server->serverid = serverid;
    server->servername = pstrdup(NameStr(serverform->srvname));
    server->owner = serverform->srvowner;
    server->fdwid = serverform->srvfdw;

    // Extract optional server type
    Datum datum = SysCacheGetAttr(FOREIGNSERVEROID, tp,
                                  Anum_pg_foreign_server_srvtype, &isnull);
    server->servertype = isnull ? NULL : TextDatumGetCString(datum);

    // Extract optional server version
    datum = SysCacheGetAttr(FOREIGNSERVEROID, tp,
                           Anum_pg_foreign_server_srvversion, &isnull);
    server->serverversion = isnull ? NULL : TextDatumGetCString(datum);

    // Extract and parse options
    datum = SysCacheGetAttr(FOREIGNSERVEROID, tp,
                           Anum_pg_foreign_server_srvoptions, &isnull);
    server->options = isnull ? NIL : untransformRelOptions(datum);

    ReleaseSysCache(tp);
    return server;
}
```