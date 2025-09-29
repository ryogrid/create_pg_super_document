# GetForeignDataWrapperExtended

## Location
[src/backend/foreign/foreign.c:49-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/foreign/foreign.c#L49-L95)

## Overview
Retrieves a foreign-data wrapper object by its Object ID (OID) with extended options for error handling, allowing callers to specify whether missing FDWs should raise an error or return NULL.

## Definition
```c
ForeignDataWrapper *GetForeignDataWrapperExtended(Oid fdwid, bits16 flags)
```

## Detailed Description
GetForeignDataWrapperExtended is the core function for looking up foreign-data wrapper objects in PostgreSQL's system catalogs. It searches the pg_foreign_data_wrapper system catalog by OID and constructs a ForeignDataWrapper structure containing all the wrapper's metadata. The function supports flexible error handling through the flags parameter - when FDW_MISSING_OK is specified, it returns NULL for non-existent wrappers instead of raising an error. The function allocates memory for the returned structure and extracts all relevant information including the wrapper's name, owner, handler function, validator function, and options.

## Parameters / Member Variables
- `fdwid`: The Object ID (OID) of the foreign-data wrapper to retrieve
- `flags`: Control flags (bits16) - when FDW_MISSING_OK is set, returns NULL instead of error for missing FDWs

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (tuple structure extraction)
  - [palloc](../p/palloc.md) (memory allocation)
  - [pstrdup](../p/pstrdup.md) (string duplication)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md) (attribute extraction)
  - [untransformRelOptions](../u/untransformRelOptions.md) (options parsing)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_foreign_data_wrapper (catalog form structure)
  - FDW_MISSING_OK (flag constant)
- Called from (representative examples):
  - [GetForeignDataWrapper](GetForeignDataWrapper.md)
  - [getObjectDescription](../g/getObjectDescription.md)
  - [getObjectIdentityParts](../g/getObjectIdentityParts.md)

## Notes and Other Information
- Located in src/backend/foreign/foreign.c:49-95
- Returns a palloc'd ForeignDataWrapper structure that must be freed by the caller
- Uses the system cache (FOREIGNDATAWRAPPEROID) for efficient lookups
- Extracts and parses fdwoptions from the catalog tuple using untransformRelOptions
- The returned structure includes: fdwid, owner, fdwname, fdwhandler, fdwvalidator, and options
- Error handling is controlled by the FDW_MISSING_OK flag in the flags parameter
- This is the primary implementation function that other FDW lookup functions delegate to

## Simplified Source

```c
ForeignDataWrapper *
GetForeignDataWrapperExtended(Oid fdwid, bits16 flags)
{
    // Look up foreign data wrapper in system catalog
    HeapTuple tp = SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(fdwid));

    // Handle missing wrapper based on flags
    if (!HeapTupleIsValid(tp)) {
        if ((flags & FDW_MISSING_OK) == 0)
            elog(ERROR, "cache lookup failed for foreign-data wrapper %u", fdwid);
        return NULL;
    }

    // Extract wrapper information from catalog tuple
    Form_pg_foreign_data_wrapper fdwform = (Form_pg_foreign_data_wrapper) GETSTRUCT(tp);

    // Allocate and populate ForeignDataWrapper structure
    ForeignDataWrapper *fdw = (ForeignDataWrapper *) palloc(sizeof(ForeignDataWrapper));
    fdw->fdwid = fdwid;
    fdw->owner = fdwform->fdwowner;
    fdw->fdwname = pstrdup(NameStr(fdwform->fdwname));
    fdw->fdwhandler = fdwform->fdwhandler;
    fdw->fdwvalidator = fdwform->fdwvalidator;

    // Extract and parse options
    Datum datum = SysCacheGetAttr(FOREIGNDATAWRAPPEROID, tp,
                                  Anum_pg_foreign_data_wrapper_fdwoptions, &isnull);
    fdw->options = isnull ? NIL : untransformRelOptions(datum);

    ReleaseSysCache(tp);
    return fdw;
}
```