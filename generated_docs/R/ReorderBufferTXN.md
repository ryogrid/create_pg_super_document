# ReorderBufferTXN

## Location
[src/include/replication/reorderbuffer.h:259-438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/reorderbuffer.h#L259-L438)

## Overview
ReorderBufferTXN represents a complete transaction in PostgreSQL's logical replication system, containing all changes, metadata, and state information necessary to reconstruct and replay the transaction.

## Definition

```c
typedef struct ReorderBufferTXN
{
	/* See above */
	bits32		txn_flags;

	/* The transaction's transaction id, can be a toplevel or sub xid. */
	TransactionId xid;

	/* Xid of top-level transaction, if known */
	TransactionId toplevel_xid;

	/*
	 * Global transaction id required for identification of prepared
	 * transactions.
	 */
	char	   *gid;

	/*
	 * LSN of the first data carrying, WAL record with knowledge about this
	 * xid. This is allowed to *not* be first record adorned with this xid, if
	 * the previous records aren't relevant for logical decoding.
	 */
	XLogRecPtr	first_lsn;

	/* ----
	 * LSN of the record that lead to this xact to be prepared or committed or
	 * aborted. This can be a
	 * * plain commit record
	 * * plain commit record, of a parent transaction
	 * * prepared transaction
	 * * prepared transaction commit
	 * * plain abort record
	 * * prepared transaction abort
	 *
	 * This can also become set to earlier values than transaction end when
	 * a transaction is spilled to disk; specifically it's set to the LSN of
	 * the latest change written to disk so far.
	 * ----
	 */
	XLogRecPtr	final_lsn;

	/*
	 * LSN pointing to the end of the commit record + 1.
	 */
	XLogRecPtr	end_lsn;

	/* Toplevel transaction for this subxact (NULL for top-level). */
	struct ReorderBufferTXN *toptxn;

	/*
	 * LSN of the last lsn at which snapshot information reside, so we can
	 * restart decoding from there and fully recover this transaction from
	 * WAL.
	 */
	XLogRecPtr	restart_decoding_lsn;

	/* origin of the change that caused this transaction */
	RepOriginId origin_id;
	XLogRecPtr	origin_lsn;

	/*
	 * Commit or Prepare time, only known when we read the actual commit or
	 * prepare record.
	 */
	union
	{
		TimestampTz commit_time;
		TimestampTz prepare_time;
		TimestampTz abort_time;
	}			xact_time;

	/*
	 * The base snapshot is used to decode all changes until either this
	 * transaction modifies the catalog, or another catalog-modifying
	 * transaction commits.
	 */
	Snapshot	base_snapshot;
	XLogRecPtr	base_snapshot_lsn;
	dlist_node	base_snapshot_node; /* link in txns_by_base_snapshot_lsn */

	/*
	 * Snapshot/CID from the previous streaming run. Only valid for already
	 * streamed transactions (NULL/InvalidCommandId otherwise).
	 */
	Snapshot	snapshot_now;
	CommandId	command_id;

	/*
	 * How many ReorderBufferChange's do we have in this txn.
	 *
	 * Changes in subtransactions are *not* included but tracked separately.
	 */
	uint64		nentries;

	/*
	 * How many of the above entries are stored in memory in contrast to being
	 * spilled to disk.
	 */
	uint64		nentries_mem;

	/*
	 * List of ReorderBufferChange structs, including new Snapshots, new
	 * CommandIds and command invalidation messages.
	 */
	dlist_head	changes;

	/*
	 * List of (relation, ctid) => (cmin, cmax) mappings for catalog tuples.
	 * Those are always assigned to the toplevel transaction. (Keep track of
	 * #entries to create a hash of the right size)
	 */
	dlist_head	tuplecids;
	uint64		ntuplecids;

	/*
	 * On-demand built hash for looking up the above values.
	 */
	HTAB	   *tuplecid_hash;

	/*
	 * Hash containing (potentially partial) toast entries. NULL if no toast
	 * tuples have been found for the current change.
	 */
	HTAB	   *toast_hash;

	/*
	 * non-hierarchical list of subtransactions that are *not* aborted. Only
	 * used in toplevel transactions.
	 */
	dlist_head	subtxns;
	uint32		nsubtxns;

	/*
	 * Stored cache invalidations. This is not a linked list because we get
	 * all the invalidations at once.
	 */
	uint32		ninvalidations;
	SharedInvalidationMessage *invalidations;

	/* ---
	 * Position in one of three lists:
	 * * list of subtransactions if we are *known* to be subxact
	 * * list of toplevel xacts (can be an as-yet unknown subxact)
	 * * list of preallocated ReorderBufferTXNs (if unused)
	 * ---
	 */
	dlist_node	node;

	/*
	 * A node in the list of catalog modifying transactions
	 */
	dlist_node	catchange_node;

	/*
	 * A node in txn_heap
	 */
	pairingheap_node txn_node;

	/*
	 * Size of this transaction (changes currently in memory, in bytes).
	 */
	Size		size;

	/* Size of top-transaction including sub-transactions. */
	Size		total_size;

	/* If we have detected concurrent abort then ignore future changes. */
	bool		concurrent_abort;

	/*
	 * Private data pointer of the output plugin.
	 */
	void	   *output_plugin_private;

	/*
	 * Stores cache invalidation messages distributed by other transactions.
	 */
	uint32		ninvalidations_distributed;
	SharedInvalidationMessage *invalidations_distributed;
} ReorderBufferTXN;
```
## Detailed Description
ReorderBufferTXN is the central data structure representing a transaction within PostgreSQL's logical replication reorder buffer system. It maintains comprehensive transaction state including all changes, snapshots, command IDs, timing information, and hierarchical relationships between parent and child transactions. The structure supports both regular transactions and prepared transactions (two-phase commit), handles toast data reconstruction, manages catalog tuple visibility mappings, and provides memory management for spilling large transactions to disk when memory limits are exceeded.

## Parameters / Member Variables
- : Bitfield containing transaction state flags and properties
- : Transaction ID of this transaction (can be toplevel or subtransaction)
- : Transaction ID of the top-level transaction if this is a subtransaction
- : Global transaction identifier for prepared transactions (two-phase commit)
- : LSN of the first WAL record carrying data for this transaction
- : LSN of the commit/prepare/abort record or latest spilled change
- : LSN pointing to the end of the commit record plus one
- : Pointer to the top-level transaction (NULL for top-level transactions)
- : LSN from which decoding can be restarted to recover this transaction
- : Replication origin identifier for change tracking
- : LSN at the origin where this change was generated
- : Timestamp when transaction was committed
- : Timestamp when transaction was prepared
- : Timestamp when transaction was aborted
- : Snapshot used for decoding changes until catalog modifications occur
- : LSN where the base snapshot was taken
- : List node for linking transactions by base snapshot LSN
- : Current snapshot for streaming transactions
- : Current command ID for streaming transactions
- : Total number of changes in this transaction (excluding subtransactions)
- : Number of changes currently stored in memory
- : Doubly-linked list of ReorderBufferChange structures
- : List of catalog tuple command ID mappings
- : Number of tuple CID mappings
- : Hash table for efficient tuple CID lookup
- : Hash table for assembling TOAST chunks
- : List of non-aborted subtransactions
- : Number of subtransactions
- : Number of cache invalidation messages
- : Array of cache invalidation messages
- : List node for transaction organization
- : List node for catalog-modifying transactions
- : Pairing heap node for transaction priority management
- : Memory size of this transaction's changes
- : Total memory size including subtransactions
- : Flag indicating concurrent abort detection
- : Private data pointer for output plugins
- : Number of distributed invalidation messages
- : Array of distributed invalidation messages

## Dependencies
- Functions called/Symbols referenced:
  - bits32
  - [ReorderBufferTXN](ReorderBufferTXN.md) (self-reference for hierarchy)
  - RepOriginId
  - [dlist_node](../d/dlist_node.md)
  - CommandId
  - [dlist_head](../d/dlist_head.md)
  - [HTAB](../H/HTAB.md)
  - [pairingheap_node](../p/pairingheap_node.md)
  - SharedInvalidationMessage
- Called from (representative examples):
  - [ReorderBufferGetTXN](ReorderBufferGetTXN.md)
  - [ReorderBufferCommit](ReorderBufferCommit.md)
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md)
  - pgoutput transaction callback functions

## Notes and Other Information
This structure is fundamental to PostgreSQL's logical replication architecture and supports advanced features like hierarchical transactions, memory management with disk spilling, TOAST data reconstruction, and two-phase commit protocols. The transaction maintains detailed timing and LSN information for proper ordering and restart capabilities. The structure efficiently handles both small and large transactions through its memory management system and supports streaming of large transactions to avoid memory exhaustion.