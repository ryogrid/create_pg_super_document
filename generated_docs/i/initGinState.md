# initGinState

## Location
[src/backend/access/gin/ginutil.c:97-225](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginutil.c#L97-L225)

## Overview
Initializes a GinState structure with index-specific information, including tuple descriptors, operator class functions, and collation settings for each indexed column.

## Definition

```c
void
initGinState(GinState *state, Relation index)
```
## Detailed Description
The  function fills in an empty  structure with all the necessary information to work with a specific GIN index. This includes setting up tuple descriptors, loading operator class support functions, and configuring collation information for each indexed column. The function handles both single-column and multi-column indexes differently, creating appropriate tuple descriptors for internal GIN storage format.

For single-column indexes, it uses the original tuple descriptor directly. For multi-column indexes, it creates a special 2-attribute tuple descriptor where the first attribute is an INT2 (column number) and the second attribute matches the original column's type.

The function loads various operator class support functions:
- Compare functions (for sorting keys)
- Extract value/query functions (for extracting searchable keys)
- Consistent/tri-consistent functions (for query matching)
- Compare partial functions (for partial matching support)

## Parameters / Member Variables
- `*state`: Pointer to the GinState structure to be initialized
- `index`: The relation representing the GIN index being initialized
## Dependencies
- Functions called/Symbols referenced:
  -  (memory initialization)
  -  (get tuple descriptor)
  -  (create tuple descriptor)
  -  (initialize tuple descriptor entries)
  -  (set collation for attributes)
  -  and  (get operator class functions)
  -  (copy function manager info)
  -  (get type information)
  -  (format type names for error messages)
  - Various GIN procedure constants: , , , , , 

- Called from:
  -  (src/backend/access/gin/ginfast.c:1079)
  -  (src/backend/access/gin/gininsert.c:335)
  -  (src/backend/access/gin/gininsert.c:499)
  -  (src/backend/access/gin/ginscan.c:45)
  -  (src/backend/access/gin/ginvacuum.c:582)
  -  (src/backend/access/gin/ginvacuum.c:706, 719)

## Notes and Other Information
- All subsidiary data is allocated in the CurrentMemoryContext
- The function handles missing compare functions by looking up the default btree comparator for the data type
- At least one of consistent or tri-consistent functions must be provided by the operator class
- Partial matching support is optional and detected by the presence of compare partial function
- For collation handling, the function uses the index's specified collation or defaults to DEFAULT_COLLATION_OID
- The function performs extensive error checking to ensure all required operator class functions are available
- The  flag optimizes handling for single-column indexes by reusing the original tuple descriptor

## Simplified Source

```c
// Simplified version of initGinState
void
initGinState(GinState *state, Relation index)
{
    TupleDesc origTupdesc = RelationGetDescr(index);

    // Initialize the state structure
    MemSet(state, 0, sizeof(GinState));
    state->index = index;
    state->oneCol = (origTupdesc->natts == 1);
    state->origTupdesc = origTupdesc;

    // Setup tuple descriptors and support functions for each column
    for (int i = 0; i < origTupdesc->natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(origTupdesc, i);

        // Create tuple descriptor for this column
        if (state->oneCol)
        {
            // Single column: use original descriptor
            state->tupdesc[i] = state->origTupdesc;
        }
        else
        {
            // Multi-column: create 2-attribute descriptor (column#, value)
            state->tupdesc[i] = CreateTemplateTupleDesc(2);
            TupleDescInitEntry(state->tupdesc[i], (AttrNumber) 1, NULL,
                              INT2OID, -1, 0);
            TupleDescInitEntry(state->tupdesc[i], (AttrNumber) 2, NULL,
                              attr->atttypid, attr->atttypmod, attr->attndims);
            TupleDescInitEntryCollation(state->tupdesc[i], (AttrNumber) 2,
                                       attr->attcollation);
        }

        // Setup compare function (from opclass or default btree comparator)
        if (index_getprocid(index, i + 1, GIN_COMPARE_PROC) != InvalidOid)
        {
            fmgr_info_copy(&(state->compareFn[i]),
                          index_getprocinfo(index, i + 1, GIN_COMPARE_PROC),
                          CurrentMemoryContext);
        }
        else
        {
            // Use default btree comparator for this type
            TypeCacheEntry *typentry = lookup_type_cache(attr->atttypid,
                                                         TYPECACHE_CMP_PROC_FINFO);
            if (!OidIsValid(typentry->cmp_proc_finfo.fn_oid))
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                               errmsg("could not identify a comparison function for type %s",
                                     format_type_be(attr->atttypid))));
            fmgr_info_copy(&(state->compareFn[i]),
                          &(typentry->cmp_proc_finfo),
                          CurrentMemoryContext);
        }

        // Setup required extract functions
        fmgr_info_copy(&(state->extractValueFn[i]),
                      index_getprocinfo(index, i + 1, GIN_EXTRACTVALUE_PROC),
                      CurrentMemoryContext);
        fmgr_info_copy(&(state->extractQueryFn[i]),
                      index_getprocinfo(index, i + 1, GIN_EXTRACTQUERY_PROC),
                      CurrentMemoryContext);

        // Setup optional tri-consistent and consistent functions
        if (index_getprocid(index, i + 1, GIN_TRICONSISTENT_PROC) != InvalidOid)
        {
            fmgr_info_copy(&(state->triConsistentFn[i]),
                          index_getprocinfo(index, i + 1, GIN_TRICONSISTENT_PROC),
                          CurrentMemoryContext);
        }

        if (index_getprocid(index, i + 1, GIN_CONSISTENT_PROC) != InvalidOid)
        {
            fmgr_info_copy(&(state->consistentFn[i]),
                          index_getprocinfo(index, i + 1, GIN_CONSISTENT_PROC),
                          CurrentMemoryContext);
        }

        // Ensure at least one consistency function is available
        if (state->consistentFn[i].fn_oid == InvalidOid &&
            state->triConsistentFn[i].fn_oid == InvalidOid)
        {
            elog(ERROR, "missing GIN support function (%d or %d) for attribute %d of index \"%s\"",
                 GIN_CONSISTENT_PROC, GIN_TRICONSISTENT_PROC,
                 i + 1, RelationGetRelationName(index));
        }

        // Setup optional partial match function
        if (index_getprocid(index, i + 1, GIN_COMPARE_PARTIAL_PROC) != InvalidOid)
        {
            fmgr_info_copy(&(state->comparePartialFn[i]),
                          index_getprocinfo(index, i + 1, GIN_COMPARE_PARTIAL_PROC),
                          CurrentMemoryContext);
            state->canPartialMatch[i] = true;
        }
        else
        {
            state->canPartialMatch[i] = false;
        }

        // Setup collation for support functions
        if (OidIsValid(index->rd_indcollation[i]))
            state->supportCollation[i] = index->rd_indcollation[i];
        else
            state->supportCollation[i] = DEFAULT_COLLATION_OID;
    }
}
```