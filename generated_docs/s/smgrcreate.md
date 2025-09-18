# smgrcreate

## Location
[src/backend/storage/smgr/smgr.c:411-425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L411-L425)

## Overview
Creates the underlying disk file or storage for a specific fork of a relation.

## Definition


## Detailed Description
This function creates the physical storage (typically a disk file) for a specific fork of a relation. It operates on an already-created but presumably unused SMgrRelation structure and delegates the actual creation to the appropriate storage manager implementation through the smgrsw function table. The function handles the creation of different types of relation forks including the main data fork, free space map (FSM), visibility map (VM), and initialization fork.

The isRedo parameter indicates whether this creation is being performed as part of WAL replay during recovery, which may affect how the storage manager handles the operation (e.g., different error handling or synchronization behavior).

## Parameters / Member Variables
- : SMgrRelation pointer representing the relation for which storage should be created
- : ForkNumber indicating which fork of the relation to create (main, FSM, VM, etc.)
- : Boolean flag indicating if this operation is part of WAL replay during recovery

## Dependencies
- Functions called/Symbols referenced:
  - smgrsw (storage manager switch table)
  - SMgrRelation (relation structure type)
  - [ForkNumber](../F/ForkNumber.md) (fork identifier type)
- Called from (representative examples):
  - [heapam_relation_set_new_filelocator](../h/heapam_relation_set_new_filelocator.md) (heap access method operations)
  - [heapam_relation_copy_data](../h/heapam_relation_copy_data.md) (heap data copying)
  - XLogReadBufferExtended (WAL replay buffer operations)
  - [index_build](../i/index_build.md) (index construction)
  - [RelationCreateStorage](../R/RelationCreateStorage.md) (relation storage creation)
  - [smgr_redo](smgr_redo.md) (storage manager WAL replay)
  - [fill_seq_with_data](../f/fill_seq_with_data.md) (sequence data initialization)
  - [ExtendBufferedRelTo](../E/ExtendBufferedRelTo.md) (buffer extension operations)
  - CreateAndCopyRelationData (relation data copying)

## Notes and Other Information
- Essential function for relation creation in PostgreSQL's storage layer
- Used during relation creation, index building, and WAL replay operations
- The isRedo parameter allows different behavior during recovery vs normal operations
- Works in conjunction with relation catalog entries but handles only the physical storage aspect
- Part of the storage manager abstraction that supports different storage implementations
- Located in src/backend/storage/smgr/smgr.c:411-425