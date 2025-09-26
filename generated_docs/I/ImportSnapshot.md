# ImportSnapshot

## Location
[src/backend/utils/time/snapmgr.c:1367-1553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L1367-L1553)

## Overview
ImportSnapshot loads a previously exported snapshot from a file and sets it as the current transaction snapshot, enabling snapshot sharing between transactions.

## Definition
```c
void ImportSnapshot(const char *idstr)
```

## Detailed Description
ImportSnapshot implements PostgreSQL's snapshot import functionality, allowing transactions to adopt a previously exported snapshot state. This function performs extensive validation to ensure snapshot compatibility and consistency:

1. **Transaction State Validation**: Ensures the function is called at the top level of a fresh transaction without any assigned XID or subtransactions
2. **Isolation Level Compatibility**: Requires SERIALIZABLE or REPEATABLE READ isolation levels
3. **File Security**: Validates the snapshot identifier format to prevent arbitrary file access
4. **Snapshot Parsing**: Reads and parses the snapshot file containing transaction visibility information
5. **Cross-Database Protection**: Prevents importing snapshots from different databases to maintain vacuum consistency
6. **Serializable Transaction Constraints**: Enforces additional restrictions for serializable transactions

The function reads snapshot data from files stored in SNAPSHOT_EXPORT_DIR, parsing various fields including transaction IDs, isolation levels, and visibility arrays.

## Parameters / Member Variables
- `idstr`: The snapshot identifier/filename to import from SNAPSHOT_EXPORT_DIR (must contain only 0-9, A-F, and hyphens)

## Dependencies
- Functions called/Symbols referenced:
  - GetTopTransactionIdIfAny
  - IsSubTransaction
  - IsolationUsesXactSnapshot
  - AllocateFile
  - parseVxidFromText
  - parseIntFromText
  - parseXidFromText
  - SetTransactionSnapshot
  - VirtualTransactionIdIsValid
  - TransactionIdIsNormal
  - IsolationIsSerializable
- Called from (representative examples):
  - ExecSetVariableStmt (for SET TRANSACTION SNAPSHOT command)

## Notes and Other Information
- Must be called before any query execution in a transaction
- Only works with SERIALIZABLE or REPEATABLE READ isolation levels
- Cannot import snapshots from different databases due to vacuum consistency requirements
- Serializable transactions have additional constraints regarding read-only status compatibility
- File format includes metadata like source vxid, pid, database ID, and isolation level
- Performs extensive validation to prevent security issues and maintain transaction consistency