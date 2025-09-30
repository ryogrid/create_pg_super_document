# index_getprocinfo

## Location
[src/backend/access/index/indexam.c:860-927](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/indexam.c#L860-L927)

## Overview
Retrieves cached function manager information for index access method support procedures, allowing index AMs to maintain fmgr lookup info for support procedures in the relation cache.

## Definition
```c
FmgrInfo *
index_getprocinfo(Relation irel,
                  AttrNumber attnum,
                  uint16 procnum)
```

## Detailed Description
This function provides index access methods with cached FmgrInfo structures for support procedures. It leverages the relation cache to store function manager lookup information, avoiding repeated lookups for "default" functions associated with indexed attributes. The function calculates the appropriate index into the cached support information array based on the attribute number and procedure number, then initializes the FmgrInfo structure if this is the first access. During initialization, it retrieves the procedure OID from the cached support procedure array, validates it exists, and sets up the function manager info along with any opclass-specific options.

The returned pointer references cached data that becomes invalid during relcache rebuilds, so callers must either use the information immediately or acquire appropriate locks on the index relation to ensure cache stability.

## Parameters / Member Variables
- `irel`: Index relation containing the cached support procedure information
- `attnum`: Attribute number (1-based) for which to retrieve the support procedure
- `procnum`: Support procedure number (1-based) within the access method's support procedure set

## Dependencies
- Functions called/Symbols referenced:
  - RegProcedureIsValid
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [RelationGetIndexAttOptions](../R/RelationGetIndexAttOptions.md)
  - [set_fn_opclass_options](../s/set_fn_opclass_options.md)
- Called from (representative examples):
  - [initGinState](initGinState.md) (GIN index initialization)
  - [initGISTstate](initGISTstate.md) (GiST index initialization)
  - [_bt_mkscankey](../b/_bt_mkscankey.md) (B-tree scan key creation)
  - [spgGetCache](../s/spgGetCache.md) (SP-GiST cache management)

## Notes and Other Information
- Only caches "default" functions for indexed attributes, not all possible support procedures
- Validates that support functions exist and raises an error if a required function is missing
- Handles opclass options initialization for non-options procedures
- The function assumes IndexSupportInitialize has already populated the rd_support array
- Critical for performance as it avoids repeated function lookups during index operations

## Simplified Source

```c
FmgrInfo *index_getprocinfo(Relation irel,
                           AttrNumber attnum,
                           uint16 procnum) {
    int nproc = irel->rd_indam->amsupport;
    int optsproc = irel->rd_indam->amoptsprocnum;

    // Validate procedure number
    Assert(procnum > 0 && procnum <= (uint16) nproc);

    // Calculate index into support info array
    int procindex = (nproc * (attnum - 1)) + (procnum - 1);

    FmgrInfo *locinfo = irel->rd_supportinfo;
    Assert(locinfo != NULL);

    locinfo += procindex;

    // Initialize lookup info if first time through
    if (locinfo->fn_oid == InvalidOid) {
        RegProcedure *loc = irel->rd_support;
        RegProcedure procId = loc[procindex];

        // Validate that support function exists
        if (!RegProcedureIsValid(procId)) {
            elog(ERROR, "missing support function %d for attribute %d of index \"%s\"",
                 procnum, attnum, RelationGetRelationName(irel));
        }

        // Initialize function manager info
        fmgr_info_cxt(procId, locinfo, irel->rd_indexcxt);

        // Set up opclass options for non-options procedures
        if (procnum != optsproc) {
            bytea **attoptions = RelationGetIndexAttOptions(irel, false);
            MemoryContext oldcxt = MemoryContextSwitchTo(irel->rd_indexcxt);
            set_fn_opclass_options(locinfo, attoptions[attnum - 1]);
            MemoryContextSwitchTo(oldcxt);
        }
    }

    return locinfo;
}
```