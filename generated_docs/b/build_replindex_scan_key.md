# build_replindex_scan_key

## Location
[src/backend/executor/execReplication.c:96-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execReplication.c#L96-L175)

## Overview
Constructs a ScanKey array for searching a relation using an index, specifically designed for replication identity operations including primary keys, replica identity, and REPLICA IDENTITY FULL indexes.

## Definition
```c
static int build_replindex_scan_key(ScanKey skey, Relation rel, Relation idxrel, TupleTableSlot *searchslot)
```

## Detailed Description
This function builds a scan key that can be used to locate a specific tuple in a relation using an index scan. It is specifically designed for replication scenarios and works with primary keys, replica identity indexes, or indexes suitable for REPLICA IDENTITY FULL tables.

The function iterates through each key attribute of the index, constructing scan key entries for non-expression attributes only. For each valid attribute, it:
1. Retrieves operator class information to determine the appropriate equality operator
2. Gets the equality strategy number for the operator class
3. Finds the corresponding equality operator function
4. Initializes a scan key entry with the appropriate strategy, function, and value
5. Sets proper collation and handles null values with special flags

The function only processes attributes that have valid table attribute numbers, skipping expression-based index columns since they are not currently supported for replication scans.

## Parameters / Member Variables
- `skey`: Output array of ScanKey structures to be populated
- `rel`: The base relation being searched (not the index relation)
- `idxrel`: The index relation to use for the scan
- `searchslot`: TupleTableSlot containing the values to search for

## Dependencies
- Functions called/Symbols referenced:
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md) (to get index operator classes)
  - IndexRelationGetNumberOfKeyAttributes (to get number of key attributes)
  - AttributeNumberIsValid (to validate attribute numbers)
  - [get_opclass_input_type](../g/get_opclass_input_type.md) (to get operator class input type)
  - [get_opclass_family](../g/get_opclass_family.md) (to get operator class family)
  - [get_equal_strategy_number](../g/get_equal_strategy_number.md) (to get equality strategy number)
  - [get_opfamily_member](../g/get_opfamily_member.md) (to find equality operator)
  - [get_opcode](../g/get_opcode.md) (to get operator function)
  - [ScanKeyInit](../S/ScanKeyInit.md) (to initialize scan key entries)

- Called from (representative examples):
  - [RelationFindReplTupleByIndex](../R/RelationFindReplTupleByIndex.md)

## Notes and Other Information
- This is a static function only accessible within execReplication.c
- Does not support expression-based index columns - only simple column references
- Requires at least one valid attribute to create a scan key (asserted)
- Properly handles null values by setting SK_ISNULL and SK_SEARCHNULL flags
- Sets appropriate collation for each scan key entry from the index definition
- Designed specifically for replication identity scenarios, not general-purpose index scanning
- Returns the number of scan key entries created (skey_attoff)

## Simplified Source

```c
static int
build_replindex_scan_key(ScanKey skey, Relation rel, Relation idxrel,
                         TupleTableSlot *searchslot)
{
    int index_attoff;
    int skey_attoff = 0;
    Datum indclassDatum;
    oidvector *opclass;
    int2vector *indkey = &idxrel->rd_index->indkey;

    // Get index operator classes
    indclassDatum = SysCacheGetAttrNotNull(INDEXRELID, idxrel->rd_indextuple,
                                         Anum_pg_index_indclass);
    opclass = (oidvector *) DatumGetPointer(indclassDatum);

    // Build scan key for each index key attribute
    for (index_attoff = 0; index_attoff < IndexRelationGetNumberOfKeyAttributes(idxrel);
         index_attoff++) {
        Oid operator;
        Oid optype;
        Oid opfamily;
        RegProcedure regop;
        int table_attno = indkey->values[index_attoff];
        StrategyNumber eq_strategy;

        // Skip expression-based index columns (not supported)
        if (!AttributeNumberIsValid(table_attno))
            continue;

        // Get operator information for equality comparison
        optype = get_opclass_input_type(opclass->values[index_attoff]);
        opfamily = get_opclass_family(opclass->values[index_attoff]);
        eq_strategy = get_equal_strategy_number(opclass->values[index_attoff]);

        operator = get_opfamily_member(opfamily, optype, optype, eq_strategy);
        if (!OidIsValid(operator))
            elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
                 eq_strategy, optype, optype, opfamily);

        regop = get_opcode(operator);

        // Initialize scan key entry
        ScanKeyInit(&skey[skey_attoff],
                   index_attoff + 1,
                   eq_strategy,
                   regop,
                   searchslot->tts_values[table_attno - 1]);

        skey[skey_attoff].sk_collation = idxrel->rd_indcollation[index_attoff];

        // Handle null values with special flags
        if (searchslot->tts_isnull[table_attno - 1])
            skey[skey_attoff].sk_flags |= (SK_ISNULL | SK_SEARCHNULL);

        skey_attoff++;
    }

    Assert(skey_attoff > 0);  // Must have at least one scan key
    return skey_attoff;
}
```