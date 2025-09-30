# AlterSequence

## Location
[src/backend/commands/sequence.c:437-540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L437-L540)

## Overview
AlterSequence modifies the definition of an existing sequence relation, handling parameter changes and optionally rewriting the sequence data when required.

## Definition

```c
structure */
	(void) read_seq_tuple(seqrel, &buf, &datatuple);
```
## Detailed Description
AlterSequence implements the ALTER SEQUENCE SQL command functionality, allowing modification of sequence parameters while maintaining data consistency and transactional safety.

The function performs these key operations:
1. **Access Control**: Uses  with ownership callback to verify permissions and lock the sequence with ShareRowExclusiveLock
2. **Data Retrieval**: 
   - Opens the sequence relation and reads current sequence data
   - Retrieves sequence metadata from pg_sequence system catalog
   - Creates a working copy of the current sequence tuple
3. **Parameter Processing**: Calls  to validate new parameters and determine if sequence rewrite is needed
4. **Conditional Rewrite**: If parameter changes require it (e.g., changing data type):
   - Gets top transaction ID for WAL purposes
   - Creates new storage file using 
   - Writes updated data using 
5. **Cache Management**: Clears local sequence cache while preserving currval() state
6. **Ownership Processing**: Handles OWNED BY clauses if specified
7. **Catalog Updates**: Updates pg_sequence catalog with new metadata
8. **Hook Invocation**: Triggers post-alter hooks for extensibility

The operation is fully transactional - if the transaction aborts, all changes are rolled back.

## Parameters / Member Variables
- : ParseState for query parsing context and error reporting
- : AlterSeqStmt containing the sequence name, options to modify, and control flags (missing_ok, for_identity)

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md)
  - [RangeVarCallbackOwnsRelation](../R/RangeVarCallbackOwnsRelation.md)
  - [init_sequence](../i/init_sequence.md)
  - SearchSysCacheCopy1
  - [read_seq_tuple](../r/read_seq_tuple.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [init_params](../i/init_params.md)
  - RelationNeedsWAL
  - [GetTopTransactionId](../G/GetTopTransactionId.md)
  - [RelationSetNewRelfilenumber](../R/RelationSetNewRelfilenumber.md)
  - [fill_seq_with_data](../f/fill_seq_with_data.md)
  - [process_owned_by](../p/process_owned_by.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - ObjectAddressSet
  - [sequence_close](../s/sequence_close.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1671)

## Notes and Other Information
- Supports missing_ok option to avoid errors when sequence doesn't exist
- Uses ShareRowExclusiveLock to allow concurrent reads but prevent conflicting alterations
- Sequence rewrites are only performed when necessary (e.g., type changes, major parameter modifications)
- Cache clearing ensures that cached sequence values don't become stale after parameter changes
- The function preserves currval() state across alterations for user session consistency
- Proper integration with the extension system and object dependency tracking
- Transaction safety ensured through proper buffer management and WAL logging

## Simplified Source

```c
ObjectAddress
AlterSequence(ParseState *pstate, AlterSeqStmt *stmt)
{
    Oid relid;
    SeqTable elm;
    Relation seqrel, rel;
    Buffer buf;
    HeapTupleData datatuple;
    Form_pg_sequence seqform;
    Form_pg_sequence_data newdataform;
    bool need_seq_rewrite;
    List *owned_by;
    ObjectAddress address;
    HeapTuple seqtuple, newdatatuple;

    // Open and lock sequence with ownership check
    relid = RangeVarGetRelidExtended(stmt->sequence, ShareRowExclusiveLock,
                                    stmt->missing_ok ? RVR_MISSING_OK : 0,
                                    RangeVarCallbackOwnsRelation, NULL);
    if (relid == InvalidOid) {
        ereport(NOTICE, (errmsg("relation \"%s\" does not exist, skipping",
                               stmt->sequence->relname)));
        return InvalidObjectAddress;
    }

    // Initialize sequence access
    init_sequence(relid, &elm, &seqrel);

    // Get sequence metadata from catalog
    rel = table_open(SequenceRelationId, RowExclusiveLock);
    seqtuple = SearchSysCacheCopy1(SEQRELID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(seqtuple))
        elog(ERROR, "cache lookup failed for sequence %u", relid);

    seqform = (Form_pg_sequence) GETSTRUCT(seqtuple);

    // Read current sequence data and make working copy
    (void) read_seq_tuple(seqrel, &buf, &datatuple);
    newdatatuple = heap_copytuple(&datatuple);
    newdataform = (Form_pg_sequence_data) GETSTRUCT(newdatatuple);
    UnlockReleaseBuffer(buf);

    // Process parameter changes
    init_params(pstate, stmt->options, stmt->for_identity, false,
               seqform, newdataform, &need_seq_rewrite, &owned_by);

    // Rewrite sequence storage if parameters require it
    if (need_seq_rewrite) {
        // Ensure WAL logging for durability
        if (RelationNeedsWAL(seqrel))
            GetTopTransactionId();

        // Create new storage file and write updated data
        RelationSetNewRelfilenumber(seqrel, seqrel->rd_rel->relpersistence);
        fill_seq_with_data(seqrel, newdatatuple);
    }

    // Clear cached sequence values (preserve currval state)
    elm->cached = elm->last;

    // Handle OWNED BY clause if specified
    if (owned_by)
        process_owned_by(seqrel, owned_by, stmt->for_identity);

    // Update catalog metadata
    CatalogTupleUpdate(rel, &seqtuple->t_self, seqtuple);

    InvokeObjectPostAlterHook(RelationRelationId, relid, 0);
    ObjectAddressSet(address, RelationRelationId, relid);

    // Cleanup
    table_close(rel, RowExclusiveLock);
    sequence_close(seqrel, NoLock);

    return address;
}
```