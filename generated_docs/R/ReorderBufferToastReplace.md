# ReorderBufferToastReplace

## Location
[src/backend/replication/logical/reorderbuffer.c:4921-5111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L4921-L5111)

## Overview
Reconstructs TOAST values from collected chunks and replaces external TOAST pointers in tuple data with in-memory reconstructed values during logical replication processing.

## Definition

```c
struct varlena *varlena;
```
## Detailed Description
This function performs the critical task of reconstructing large column values that were stored using PostgreSQL's TOAST mechanism during logical replication. When a transaction is ready for processing, any external TOAST pointers in the tuple data need to be replaced with the actual reconstructed values built from the chunks collected by ReorderBufferToastAppendChunk. The function iterates through all attributes in the tuple, identifies external TOAST pointers, looks up the corresponding chunks in the transaction's toast hash table, reassembles the chunks into the original large value, and replaces the external pointer with an indirect pointer to the reconstructed data. It also carefully manages memory accounting by tracking the size difference between the original change and the modified change with reconstructed TOAST data.

## Parameters / Member Variables
- : Pointer to the ReorderBuffer containing memory context and configuration
- : Pointer to the ReorderBufferTXN containing the toast_hash with collected chunks
- : The base relation (not the TOAST relation) being processed
- : The ReorderBufferChange containing the tuple to be modified

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferChangeSize](ReorderBufferChangeSize.md) (calculates change size for memory accounting)
  - [RelationIdGetRelation](RelationIdGetRelation.md), RelationClose (accesses TOAST relation)
  - [heap_deform_tuple](../h/heap_deform_tuple.md), heap_form_tuple (tuple manipulation)
  - VARATT_IS_EXTERNAL, VARATT_EXTERNAL_GET_POINTER (TOAST pointer analysis)
  - [hash_search](../h/hash_search.md) (finds TOAST entries in hash table)
  - dlist_foreach, dlist_container (iterates through chunk list)
  - SET_VARTAG_EXTERNAL, VARDATA_EXTERNAL (creates indirect pointers)
  - [ReorderBufferChangeMemoryUpdate](ReorderBufferChangeMemoryUpdate.md) (updates memory accounting)
  - Various TOAST macros for size and compression handling
- Called from (representative examples):
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md) (during transaction commit processing)

## Notes and Other Information
- Only processes changes that have collected TOAST chunks (txn->toast_hash != NULL)
- Handles both compressed and uncompressed TOAST values appropriately  
- Creates indirect pointers to reconstructed data rather than inline storage
- Carefully manages memory accounting to prevent serialization triggers during commit
- Validates chunk data integrity during reconstruction (no external or short chunks)
- Allocates reconstructed data in the reorder buffer's memory context
- Critical for ensuring large column values are available during logical replication output
- The function is static, used only within the reorder buffer implementation
- Must be called after all TOAST chunks for a transaction have been collected

## Simplified Source

```c
static void
ReorderBufferToastReplace(ReorderBuffer *rb, ReorderBufferTXN *txn,
						  Relation relation, ReorderBufferChange *change)
{
	// Early return if no toast tuples changed
	if (txn->toast_hash == NULL)
		return;

	// Record old size for memory accounting
	Size old_size = ReorderBufferChangeSize(change);
	MemoryContext oldcontext = MemoryContextSwitchTo(rb->context);

	HeapTuple newtup = change->data.tp.newtuple;
	TupleDesc desc = RelationGetDescr(relation);

	// Get TOAST relation descriptor
	Relation toast_rel = RelationIdGetRelation(relation->rd_rel->reltoastrelid);
	TupleDesc toast_desc = RelationGetDescr(toast_rel);

	// Allocate arrays for tuple reconstruction
	Datum *attrs = palloc0(sizeof(Datum) * desc->natts);
	bool *isnull = palloc0(sizeof(bool) * desc->natts);
	bool *free = palloc0(sizeof(bool) * desc->natts);

	// Deform original tuple
	heap_deform_tuple(newtup, desc, attrs, isnull);

	// Process each attribute to replace TOAST pointers
	for (int natt = 0; natt < desc->natts; natt++) {
		Form_pg_attribute attr = TupleDescAttr(desc, natt);

		// Skip non-varlena or null attributes
		if (attr->attnum < 0 || attr->attisdropped ||
			attr->attlen != -1 || isnull[natt])
			continue;

		struct varlena *varlena = (struct varlena *) DatumGetPointer(attrs[natt]);

		// Skip non-external TOAST pointers
		if (!VARATT_IS_EXTERNAL(varlena))
			continue;

		// Extract TOAST pointer and find corresponding entry
		struct varatt_external toast_pointer;
		VARATT_EXTERNAL_GET_POINTER(toast_pointer, varlena);

		ReorderBufferToastEnt *ent = hash_search(txn->toast_hash,
												 &toast_pointer.va_valueid, HASH_FIND, NULL);
		if (ent == NULL)
			continue;

		// Reconstruct TOAST value from chunks
		struct varlena *reconstructed = palloc0(toast_pointer.va_rawsize);
		ent->reconstructed = reconstructed;

		// Stitch chunks together
		Size data_done = 0;
		dlist_iter it;
		dlist_foreach(it, &ent->chunks) {
			ReorderBufferChange *cchange = dlist_container(ReorderBufferChange, node, it.cur);
			HeapTuple ctup = cchange->data.tp.newtuple;
			bool cisnull;
			Pointer chunk = DatumGetPointer(fastgetattr(ctup, 3, toast_desc, &cisnull));

			memcpy(VARDATA(reconstructed) + data_done, VARDATA(chunk),
				   VARSIZE(chunk) - VARHDRSZ);
			data_done += VARSIZE(chunk) - VARHDRSZ;
		}

		// Set proper size and compression flags
		if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
			SET_VARSIZE_COMPRESSED(reconstructed, data_done + VARHDRSZ);
		else
			SET_VARSIZE(reconstructed, data_done + VARHDRSZ);

		// Create indirect pointer to reconstructed data
		struct varlena *new_datum = palloc0(INDIRECT_POINTER_SIZE);
		struct varatt_indirect redirect_pointer;
		redirect_pointer.pointer = reconstructed;

		SET_VARTAG_EXTERNAL(new_datum, VARTAG_INDIRECT);
		memcpy(VARDATA_EXTERNAL(new_datum), &redirect_pointer, sizeof(redirect_pointer));

		attrs[natt] = PointerGetDatum(new_datum);
		free[natt] = true;
	}

	// Rebuild tuple and copy back
	HeapTuple tmphtup = heap_form_tuple(desc, attrs, isnull);
	memcpy(newtup->t_data, tmphtup->t_data, tmphtup->t_len);
	newtup->t_len = tmphtup->t_len;

	// Cleanup resources
	RelationClose(toast_rel);
	pfree(tmphtup);
	for (int natt = 0; natt < desc->natts; natt++) {
		if (free[natt])
			pfree(DatumGetPointer(attrs[natt]));
	}
	pfree(attrs);
	pfree(free);
	pfree(isnull);

	MemoryContextSwitchTo(oldcontext);

	// Update memory accounting
	ReorderBufferChangeMemoryUpdate(rb, change, NULL, false, old_size);
	ReorderBufferChangeMemoryUpdate(rb, change, NULL, true, ReorderBufferChangeSize(change));
}
```