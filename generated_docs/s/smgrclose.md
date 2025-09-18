# smgrclose

## Location
[src/backend/storage/smgr/smgr.c:320-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L320-L331)

## Overview
Closes an SMgrRelation object by releasing its resources, serving as a high-level interface for relation cleanup.

## Definition


## Detailed Description
The  function provides a conceptual "close" operation for SMgrRelation objects. According to the implementation comments, this function is currently implemented as a simple wrapper around  because PostgreSQL doesn't track all references to SMgrRelation objects returned by . Since multiple references to the same object may exist, the function cannot safely destroy the object and instead just releases its resources. The SMgrRelation object remains in the storage manager's data structures and can potentially be reused.

## Parameters / Member Variables
- : Pointer to the SMgrRelation object to be closed. The reference should not be used after this call, though the object itself may remain valid.

## Dependencies
- Functions called/Symbols referenced:
  - [smgrrelease](smgrrelease.md) (performs the actual resource cleanup)
- Called from (representative examples):
  - [heapam_relation_set_new_filelocator](../h/heapam_relation_set_new_filelocator.md)
  - [heapam_relation_copy_data](../h/heapam_relation_copy_data.md)
  - [smgrDoPendingDeletes](smgrDoPendingDeletes.md)
  - [ScanSourceDatabasePgClass](../S/ScanSourceDatabasePgClass.md)
  - [fill_seq_with_data](../f/fill_seq_with_data.md)
  - [index_copy_data](../i/index_copy_data.md)
  - [DropRelationFiles](../D/DropRelationFiles.md)
  - [RelationSetNewRelfilenumber](../R/RelationSetNewRelfilenumber.md)
  - RelationCloseSmgr

## Notes and Other Information
- This is a public function available to other modules throughout PostgreSQL
- Currently implemented as a synonym for smgrrelease() due to reference tracking limitations
- The function comment warns that the SMgrRelation reference should not be used after this call
- Called from various parts of the system including heap access methods, catalog operations, and relation management
- The design reflects PostgreSQL's approach to resource management where objects may persist longer than their logical lifetime
- This function represents the intended API for closing relations, even though the implementation is currently identical to smgrrelease