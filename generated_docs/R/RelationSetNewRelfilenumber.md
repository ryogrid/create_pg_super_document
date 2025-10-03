# RelationSetNewRelfilenumber

## Location
[src/backend/utils/cache/relcache.c:3769-3970](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L3769-L3970)

## Overview
RelationSetNewRelfilenumber assigns a new physical file number (and optionally new persistence setting) to a relation, enabling transactionally safe full rewrites of relations.

## Definition

```c
enumber(Relation relation, char persistence)
{
	RelFileNumber newrelfilenumber;
	Relation	pg_class;
	ItemPointerData otid;
	HeapTuple	tuple;
	Form_pg_class classform;
	MultiXactId minmulti = InvalidMultiXactId;
	TransactionId freezeXid = InvalidTransactionId;
	RelFileLocator newrlocator;

	if (!IsBinaryUpgrade)
	{
		/* Allocate a new relfilenumber */
		newrelfilenumber = GetNewRelFileNumber(relation->rd_rel->reltablespace,
											   NULL, persistence);
	}
	else if (relation->rd_rel->relkind == RELKIND_INDEX)
	{
		if (!OidIsValid(binary_upgrade_next_index_pg_class_relfilenumber))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("index relfilenumber value not set when in binary upgrade mode")));

		newrelfilenumber = binary_upgrade_next_index_pg_class_relfilenumber;
		binary_upgrade_next_index_pg_class_relfilenumber = InvalidOid;
	}
	else if (relation->rd_rel->relkind == RELKIND_RELATION)
	{
		if (!OidIsValid(binary_upgrade_next_heap_pg_class_relfilenumber))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("heap relfilenumber value not set when in binary upgrade mode")));

		newrelfilenumber = binary_upgrade_next_heap_pg_class_relfilenumber;
		binary_upgrade_next_heap_pg_class_relfilenumber = InvalidOid;
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unexpected request for new relfilenumber in binary upgrade mode")));

	/*
	 * Get a writable copy of the pg_class tuple for the given relation.
	 */
	pg_class = table_open(RelationRelationId, RowExclusiveLock);

	tuple = SearchSysCacheLockedCopy1(RELOID,
									  ObjectIdGetDatum(RelationGetRelid(relation)));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for relation %u",
			 RelationGetRelid(relation));
	otid = tuple->t_self;
	classform = (Form_pg_class) GETSTRUCT(tuple);

	/*
	 * Schedule unlinking of the old storage at transaction commit, except
	 * when performing a binary upgrade, when we must do it immediately.
	 */
	if (IsBinaryUpgrade)
	{
		SMgrRelation srel;

		/*
		 * During a binary upgrade, we use this code path to ensure that
		 * pg_largeobject and its index have the same relfilenumbers as in the
		 * old cluster. This is necessary because pg_upgrade treats
		 * pg_largeobject like a user table, not a system table. It is however
		 * possible that a table or index may need to end up with the same
		 * relfilenumber in the new cluster as what it had in the old cluster.
		 * Hence, we can't wait until commit time to remove the old storage.
		 *
		 * In general, this function needs to have transactional semantics,
		 * and removing the old storage before commit time surely isn't.
		 * However, it doesn't really matter, because if a binary upgrade
		 * fails at this stage, the new cluster will need to be recreated
		 * anyway.
		 */
		srel = smgropen(relation->rd_locator, relation->rd_backend);
		smgrdounlinkall(&srel, 1, false);
		smgrclose(srel);
	}
	else
	{
		/* Not a binary upgrade, so just schedule it to happen later. */
		RelationDropStorage(relation);
	}

	/*
	 * Create storage for the main fork of the new relfilenumber.  If it's a
	 * table-like object, call into the table AM to do so, which'll also
	 * create the table's init fork if needed.
	 *
	 * NOTE: If relevant for the AM, any conflict in relfilenumber value will
	 * be caught here, if GetNewRelFileNumber messes up for any reason.
	 */
	newrlocator = relation->rd_locator;
	newrlocator.relNumber = newrelfilenumber;

	if (RELKIND_HAS_TABLE_AM(relation->rd_rel->relkind))
	{
		table_relation_set_new_filelocator(relation, &newrlocator,
										   persistence,
										   &freezeXid, &minmulti);
	}
	else if (RELKIND_HAS_STORAGE(relation->rd_rel->relkind))
	{
		/* handle these directly, at least for now */
		SMgrRelation srel;

		srel = RelationCreateStorage(newrlocator, persistence, true);
		smgrclose(srel);
	}
	else
	{
		/* we shouldn't be called for anything else */
		elog(ERROR, "relation \"%s\" does not have storage",
			 RelationGetRelationName(relation));
	}

	/*
	 * If we're dealing with a mapped index, pg_class.relfilenode doesn't
	 * change; instead we have to send the update to the relation mapper.
	 *
	 * For mapped indexes, we don't actually change the pg_class entry at all;
	 * this is essential when reindexing pg_class itself.  That leaves us with
	 * possibly-inaccurate values of relpages etc, but those will be fixed up
	 * later.
	 */
	if (RelationIsMapped(relation))
	{
		/* This case is only supported for indexes */
		Assert(relation->rd_rel->relkind == RELKIND_INDEX);

		/* Since we're not updating pg_class, these had better not change */
		Assert(classform->relfrozenxid == freezeXid);
		Assert(classform->relminmxid == minmulti);
		Assert(classform->relpersistence == persistence);

		/*
		 * In some code paths it's possible that the tuple update we'd
		 * otherwise do here is the only thing that would assign an XID for
		 * the current transaction.  However, we must have an XID to delete
		 * files, so make sure one is assigned.
		 */
		(void) GetCurrentTransactionId();

		/* Do the deed */
		RelationMapUpdateMap(RelationGetRelid(relation),
							 newrelfilenumber,
							 relation->rd_rel->relisshared,
							 false);

		/* Since we're not updating pg_class, must trigger inval manually */
		CacheInvalidateRelcache(relation);
	}
	else
	{
		/* Normal case, update the pg_class entry */
		classform->relfilenode = newrelfilenumber;

		/* relpages etc. never change for sequences */
		if (relation->rd_rel->relkind != RELKIND_SEQUENCE)
		{
			classform->relpages = 0;	/* it's empty until further notice */
			classform->reltuples = -1;
			classform->relallvisible = 0;
		}
		classform->relfrozenxid = freezeXid;
		classform->relminmxid = minmulti;
		classform->relpersistence = persistence;

		CatalogTupleUpdate(pg_class, &otid, tuple);
	}

	UnlockTuple(pg_class, &otid, InplaceUpdateTupleLock);
	heap_freetuple(tuple);

	table_close(pg_class, RowExclusiveLock);

	/*
	 * Make the pg_class row change or relation map change visible.  This will
	 * cause the relcache entry to get updated, too.
	 */
	CommandCounterIncrement();

	RelationAssumeNewRelfilelocator(relation);
}

/*
 * RelationAssumeNewRelfilelocator
 *
 * Code that modifies pg_class.reltablespace or pg_class.relfilenode must call
 * this.  The call shall precede any code that might insert WAL records whose
 * replay would modify bytes in the new RelFileLocator, and the call shall follow
 * any WAL modifying bytes in the prior RelFileLocator.  See struct RelationData.
 * Ideally, call this as near as possible to the CommandCounterIncrement()
 * that makes the pg_class change visible (before it or after it);
```
## Detailed Description
This function performs a complete relfilenumber change operation for a relation, which effectively creates new physical storage while maintaining transactional safety. The process involves several coordinated steps:

1. **File Number Allocation**: Either allocates a new relfilenumber via GetNewRelFileNumber() or uses pre-assigned numbers during binary upgrades
2. **Catalog Updates**: Updates the pg_class catalog entry with the new relfilenumber and related statistics
3. **Storage Management**: Creates new physical storage and schedules the old storage for deletion at transaction commit
4. **Mapping Handling**: For mapped relations (system catalogs), updates the relation mapping instead of pg_class.relfilenode
5. **Statistics Reset**: Resets relation statistics (relpages, reltuples, relallvisible) since the new storage starts empty
6. **Transaction Integration**: Ensures proper XID assignment for file deletion operations and cache invalidation

The function handles different relation types appropriately, using table access methods for table-like objects and direct storage creation for others. For binary upgrades, it immediately removes old storage rather than deferring to commit time.

Special handling exists for mapped relations where pg_class.relfilenode doesn't change, and updates go through the relation mapper instead. This is essential when reindexing system catalogs like pg_class itself.

## Parameters / Member Variables
- `relation`: The relation to assign a new relfilenumber to
- `persistence`: New persistence setting (permanent, temporary, or unlogged)
## Dependencies
- Functions called/Symbols referenced:
  - [GetNewRelFileNumber](../G/GetNewRelFileNumber.md)
  - [SearchSysCacheLockedCopy1](../S/SearchSysCacheLockedCopy1.md)
  - [RelationDropStorage](RelationDropStorage.md)
  - [RelationCreateStorage](RelationCreateStorage.md)
  - [table_relation_set_new_filelocator](../t/table_relation_set_new_filelocator.md)
  - [RelationMapUpdateMap](RelationMapUpdateMap.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - [RelationAssumeNewRelfilelocator](RelationAssumeNewRelfilelocator.md)
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [reindex_index](../r/reindex_index.md)
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md)
  - [ResetSequence](ResetSequence.md)
  - [AlterSequence](../A/AlterSequence.md)
  - [SequenceChangePersistence](../S/SequenceChangePersistence.md)

## Notes and Other Information
- Caller must hold exclusive lock on the relation before calling this function
- The operation limits access to the relation's old data for the remainder of the current transaction
- Binary upgrade mode uses pre-assigned relfilenumbers instead of allocating new ones
- For mapped relations, pg_class statistics may become temporarily inaccurate but will be corrected later
- Sequences preserve their relpages/reltuples statistics since they don't change during the operation
- The function ensures transactional safety by scheduling old storage deletion for commit time (except during binary upgrades)
- Table access methods handle creation of both main fork and initialization fork as needed
- The operation triggers cache invalidation to ensure the relcache reflects the new relfilenumber

## Simplified Source

```c
void
RelationSetNewRelfilenumber(Relation relation, char persistence)
{
    RelFileNumber newrelfilenumber;
    Relation pg_class;
    HeapTuple tuple;
    Form_pg_class classform;
    TransactionId freezeXid = InvalidTransactionId;
    MultiXactId minmulti = InvalidMultiXactId;
    RelFileLocator newrlocator;

    // Allocate new relfilenumber (or use pre-assigned for binary upgrade)
    if (!IsBinaryUpgrade)
    {
        newrelfilenumber = GetNewRelFileNumber(relation->rd_rel->reltablespace,
                                               NULL, persistence);
    }
    else
    {
        // Binary upgrade mode: use pre-assigned relfilenumber
        if (relation->rd_rel->relkind == RELKIND_INDEX)
            newrelfilenumber = binary_upgrade_next_index_pg_class_relfilenumber;
        else if (relation->rd_rel->relkind == RELKIND_RELATION)
            newrelfilenumber = binary_upgrade_next_heap_pg_class_relfilenumber;
        else
            ereport(ERROR, (errmsg("unexpected request for new relfilenumber")));
    }

    // Get pg_class tuple for this relation
    pg_class = table_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheLockedCopy1(RELOID,
                                      ObjectIdGetDatum(RelationGetRelid(relation)));
    classform = (Form_pg_class) GETSTRUCT(tuple);

    // Schedule old storage deletion (immediate for binary upgrade)
    if (IsBinaryUpgrade)
    {
        SMgrRelation srel = smgropen(relation->rd_locator, relation->rd_backend);
        smgrdounlinkall(&srel, 1, false);
        smgrclose(srel);
    }
    else
        RelationDropStorage(relation);

    // Create new storage
    newrlocator = relation->rd_locator;
    newrlocator.relNumber = newrelfilenumber;

    if (RELKIND_HAS_TABLE_AM(relation->rd_rel->relkind))
    {
        table_relation_set_new_filelocator(relation, &newrlocator,
                                           persistence, &freezeXid, &minmulti);
    }
    else if (RELKIND_HAS_STORAGE(relation->rd_rel->relkind))
    {
        SMgrRelation srel = RelationCreateStorage(newrlocator, persistence, true);
        smgrclose(srel);
    }

    // Update catalog or relation mapper
    if (RelationIsMapped(relation))
    {
        // For mapped relations, update relation mapper
        (void) GetCurrentTransactionId(); // Ensure XID for file deletion
        RelationMapUpdateMap(RelationGetRelid(relation),
                             newrelfilenumber,
                             relation->rd_rel->relisshared,
                             false);
        CacheInvalidateRelcache(relation);
    }
    else
    {
        // Normal case: update pg_class entry
        classform->relfilenode = newrelfilenumber;

        // Reset statistics for non-sequences
        if (relation->rd_rel->relkind != RELKIND_SEQUENCE)
        {
            classform->relpages = 0;
            classform->reltuples = -1;
            classform->relallvisible = 0;
        }
        classform->relfrozenxid = freezeXid;
        classform->relminmxid = minmulti;
        classform->relpersistence = persistence;

        CatalogTupleUpdate(pg_class, &tuple->t_self, tuple);
    }

    // Cleanup and finalize
    heap_freetuple(tuple);
    table_close(pg_class, RowExclusiveLock);
    CommandCounterIncrement();
    RelationAssumeNewRelfilelocator(relation);
}
```