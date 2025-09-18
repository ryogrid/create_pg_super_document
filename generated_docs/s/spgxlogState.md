# spgxlogState

## Location
[src/include/access/spgxlog.h:36-40](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgxlog.h#L36-L40)

## Overview
The spgxlogState structure carries essential state information required by SP-GiST redo functions during WAL (Write-Ahead Logging) recovery operations.

## Definition
```c
typedef struct spgxlogState
{
    TransactionId redirectXid;
    bool          isBuild;
} spgxlogState;
```

## Detailed Description
The spgxlogState structure is a lightweight container designed to provide SP-GiST redo functions with the minimal state information needed during WAL recovery. While some redo functions require an SpGistState structure, only a few fields of that larger structure are actually necessary during recovery operations. The spgxlogState serves as an optimized alternative that includes only the essential fields, reducing the overhead of WAL records while maintaining the functionality required for proper recovery.

## Parameters / Member Variables
- `redirectXid`: Transaction ID used for redirect tuples during SP-GiST operations
- `isBuild`: Boolean flag indicating whether the operation is part of an index build process

## Dependencies
- Functions called/Symbols referenced: 
  - TransactionId (PostgreSQL transaction identifier type)
- Called from (representative examples):
  - [fillFakeState](../f/fillFakeState.md) (in spgxlog.c:35)
  - [spgxlogMoveLeafs](spgxlogMoveLeafs.md) (in spgxlog.h:75)
  - [spgxlogAddNode](spgxlogAddNode.md) (in spgxlog.h:130)
  - [spgxlogPickSplit](spgxlogPickSplit.md) (in spgxlog.h:185)
  - [spgxlogVacuumLeaf](spgxlogVacuumLeaf.md) (in spgxlog.h:208)
  - [spgxlogVacuumRoot](spgxlogVacuumRoot.md) (in spgxlog.h:230)

## Notes and Other Information
- This structure is specifically designed for WAL recovery scenarios and should not be confused with the full SpGistState structure used during normal operations
- The fillFakeState function in spgxlog.c contains additional comments explaining the usage and rationale for this structure
- The structure is kept minimal to reduce WAL record size while providing necessary recovery context