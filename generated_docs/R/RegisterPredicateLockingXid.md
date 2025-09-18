# RegisterPredicateLockingXid

## Location
src/backend/storage/lmgr/predicate.c: 1949 - 1997

## Overview
Registers a transaction ID (XID) in the global SerializableXidHash for predicate lock tracking, creating the mapping between XIDs and their corresponding serializable transaction structures.

## Definition
```c
void RegisterPredicateLockingXid(TransactionId xid)
```

## Detailed Description
This function creates the essential mapping between a transaction's XID (Transaction ID) and its SERIALIZABLEXACT structure in PostgreSQL's Serializable Snapshot Isolation system. When a serializable transaction is first assigned an XID, this function:

1. **Validates Context**: Ensures the transaction is actually tracking predicate locks (MySerializableXact is valid)
2. **Records XID**: Stores the XID in the transaction's SERIALIZABLEXACT structure for future reference
3. **Creates Global Mapping**: Adds an entry to SerializableXidHash that maps the XID to the SERIALIZABLEXACT structure
4. **Ensures Uniqueness**: Asserts that this XID hasn't already been registered (should only be called once per transaction)

This mapping is crucial for the predicate locking system because other parts of the system need to be able to look up a transaction's serializable state given only its XID. The mapping enables efficient conflict detection and resolution during the serializable isolation protocol.

## Parameters / Member Variables
- `xid`: The transaction ID to register - must be a valid XID and represent the top-level transaction

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md) (PostgreSQL hash table search/insert function)
  - [SERIALIZABLEXIDTAG](../S/SERIALIZABLEXIDTAG.md) (key structure for XID hash table)
  - [SERIALIZABLEXID](../S/SERIALIZABLEXID.md) (entry structure in XID hash table)
  - HASH_ENTER (hash operation flag for insert)
  - InvalidSerializableXact (constant for uninitialized serializable transactions)
- Called from (representative examples):
  - [AssignTransactionId](../A/AssignTransactionId.md) (in src/backend/access/transam/xact.c:716 when XID is first assigned)

## Notes and Other Information
- Must only be called once per transaction - includes assertion to prevent duplicate registration
- Requires that MySerializableXact is already initialized (typically done during snapshot acquisition)
- The XID must be valid and represent a top-level transaction (not a subtransaction)
- Uses SerializableXactHashLock to ensure thread-safe access to the global hash table
- The mapping created here is essential for other transactions to find this transaction's serializable state during conflict detection
- Part of PostgreSQL's Serializable Snapshot Isolation (SSI) implementation
- The hash table entry will be cleaned up when the transaction completes and its serializable state is released