# RelationBuildPartitionKey

## Location
[src/backend/utils/cache/partcache.c:78-276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/partcache.c#L78-L276)

## Overview
Constructs and caches the complete partition key data structure for a partitioned table by reading metadata from system catalogs and building all necessary type and operator information.

## Definition
```c
static void RelationBuildPartitionKey(Relation relation)
```

## Detailed Description
RelationBuildPartitionKey is a complex internal function that builds the complete PartitionKey structure for a partitioned table. The function retrieves partition metadata from the pg_partitioned_table catalog and constructs a comprehensive data structure containing all information needed for partitioning operations.

The function implements careful memory management by creating a dedicated memory context ("partition key") as a child of CurTransactionContext initially, then reparenting it to CacheMemoryContext only after successful completion. This prevents memory leaks if errors occur during construction.

Key operations include:
- Reading partition strategy, attribute count, and attribute numbers from pg_partitioned_table
- Processing operator classes and collations for each partition attribute  
- Parsing and optimizing partition expressions (for expression-based partitioning)
- Looking up support functions for comparison/hashing operations
- Collecting complete type information for each partition column
- Validating partition strategy and operator class compatibility

## Parameters / Member Variables
- `relation`: The partitioned table relation for which to build the partition key

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (catalog lookup)
  - AllocSetContextCreate (memory context creation)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md) (retrieving operator classes and collations)
  - [stringToNode](../s/stringToNode.md) (parsing partition expressions)
  - [eval_const_expressions](../e/eval_const_expressions.md) (optimizing expressions)
  - [fix_opfuncids](../f/fix_opfuncids.md) (fixing operator function IDs)
  - [get_opfamily_proc](../g/get_opfamily_proc.md) (finding support functions)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md) (caching function information)
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md) (type information)
  - [MemoryContextSetParent](../M/MemoryContextSetParent.md) (memory management)
- Called from:
  - [RelationGetPartitionKey](RelationGetPartitionKey.md) (when rd_partkey is NULL)

## Notes and Other Information
- Function is static (internal to partcache.c)
- Creates dedicated memory context to avoid complex cleanup logic
- Supports all partition strategies: LIST, RANGE, and HASH
- Handles both column-based and expression-based partitioning
- Validates operator class support for the chosen partition strategy
- Uses different support function numbers based on strategy (HASHEXTENDED_PROC for hash, BTORDER_PROC for others)
- Performs const-simplification on partition expressions for planner compatibility
- Memory context is initially created under CurTransactionContext and only reparented to CacheMemoryContext on success
- Results are cached in relation->rd_partkey and persist until relation is closed

## Simplified Source

```c
// Simplified version of RelationBuildPartitionKey
static void RelationBuildPartitionKey(Relation relation) {
    Form_pg_partitioned_table form;
    HeapTuple tuple;
    PartitionKey key;
    MemoryContext partkeycxt;

    // Step 1: Look up partition metadata in system catalog
    tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(RelationGetRelid(relation)));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for partition key of relation %u",
             RelationGetRelid(relation));

    // Step 2: Create dedicated memory context for partition key
    partkeycxt = AllocSetContextCreate(CurTransactionContext, "partition key",
                                      ALLOCSET_SMALL_SIZES);
    key = (PartitionKey) MemoryContextAllocZero(partkeycxt, sizeof(PartitionKeyData));

    // Step 3: Extract basic partition information
    form = (Form_pg_partitioned_table) GETSTRUCT(tuple);
    key->strategy = form->partstrat;  // LIST, RANGE, or HASH
    key->partnatts = form->partnatts; // Number of partition attributes

    // Validate partition strategy
    if (key->strategy != PARTITION_STRATEGY_LIST &&
        key->strategy != PARTITION_STRATEGY_RANGE &&
        key->strategy != PARTITION_STRATEGY_HASH)
        elog(ERROR, "invalid partition strategy \"%c\"", key->strategy);

    // Step 4: Get operator classes and collations from catalog
    oidvector *opclass = (oidvector *) DatumGetPointer(
        SysCacheGetAttrNotNull(PARTRELID, tuple, Anum_pg_partitioned_table_partclass));
    oidvector *collation = (oidvector *) DatumGetPointer(
        SysCacheGetAttrNotNull(PARTRELID, tuple, Anum_pg_partitioned_table_partcollation));

    // Step 5: Process partition expressions if any exist
    Datum expr_datum = SysCacheGetAttr(PARTRELID, tuple,
                                      Anum_pg_partitioned_table_partexprs, &isnull);
    if (!isnull) {
        char *exprString = TextDatumGetCString(expr_datum);
        Node *expr = stringToNode(exprString);

        // Optimize expressions for planner compatibility
        expr = eval_const_expressions(NULL, expr);
        fix_opfuncids(expr);

        // Copy to partition context
        MemoryContext oldcxt = MemoryContextSwitchTo(partkeycxt);
        key->partexprs = (List *) copyObject(expr);
        MemoryContextSwitchTo(oldcxt);
    }

    // Step 6: Allocate arrays for per-attribute information
    MemoryContext oldcxt = MemoryContextSwitchTo(partkeycxt);
    key->partattrs = palloc0(key->partnatts * sizeof(AttrNumber));
    key->partopfamily = palloc0(key->partnatts * sizeof(Oid));
    key->partsupfunc = palloc0(key->partnatts * sizeof(FmgrInfo));
    key->partcollation = palloc0(key->partnatts * sizeof(Oid));
    key->parttypid = palloc0(key->partnatts * sizeof(Oid));
    // ... other type arrays ...
    MemoryContextSwitchTo(oldcxt);

    // Step 7: Process each partition attribute
    int16 support_func_num = (key->strategy == PARTITION_STRATEGY_HASH) ?
                            HASHEXTENDED_PROC : BTORDER_PROC;

    for (int i = 0; i < key->partnatts; i++) {
        AttrNumber attno = form->partattrs.values[i];

        // Get operator class information
        HeapTuple opclasstup = SearchSysCache1(CLAOID,
                                              ObjectIdGetDatum(opclass->values[i]));
        Form_pg_opclass opclassform = (Form_pg_opclass) GETSTRUCT(opclasstup);

        key->partopfamily[i] = opclassform->opcfamily;

        // Find support function for this operator family
        Oid funcid = get_opfamily_proc(opclassform->opcfamily,
                                      opclassform->opcintype,
                                      opclassform->opcintype,
                                      support_func_num);
        fmgr_info_cxt(funcid, &key->partsupfunc[i], partkeycxt);

        // Store collation
        key->partcollation[i] = collation->values[i];

        // Get type information from attribute or expression
        if (attno != 0) {
            // Regular column attribute
            Form_pg_attribute att = TupleDescAttr(relation->rd_att, attno - 1);
            key->parttypid[i] = att->atttypid;
            key->parttypmod[i] = att->atttypmod;
        } else {
            // Expression-based partition key
            Node *expr_node = lfirst(partexprs_item);
            key->parttypid[i] = exprType(expr_node);
            key->parttypmod[i] = exprTypmod(expr_node);
        }

        // Get type properties (length, by-value, alignment)
        get_typlenbyvalalign(key->parttypid[i], &key->parttyplen[i],
                            &key->parttypbyval[i], &key->parttypalign[i]);

        ReleaseSysCache(opclasstup);
    }

    ReleaseSysCache(tuple);

    // Step 8: Success - attach to relation and reparent memory context
    MemoryContextSetParent(partkeycxt, CacheMemoryContext);
    relation->rd_partkeycxt = partkeycxt;
    relation->rd_partkey = key;
}
```

Key simplifications made:
- Removed detailed error handling and validation for clarity
- Consolidated variable declarations and initialization
- Simplified memory context switching patterns
- Focused on the main execution path through all steps
- Added clear step-by-step comments explaining each phase
- Abstracted some low-level pointer arithmetic and array operations
- Removed platform-specific optimizations and edge case handling