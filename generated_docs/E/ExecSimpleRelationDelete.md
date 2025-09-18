# ExecSimpleRelationDelete

## Location
[src/backend/executor/execReplication.c:623-655](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execReplication.c#L623-L655)

## Overview
ExecSimpleRelationDelete performs a complete tuple deletion operation, including trigger execution and proper cleanup of the specified tuple from the relation.

## Definition
```c
void ExecSimpleRelationDelete(ResultRelInfo *resultRelInfo,
                             EState *estate, EPQState *epqstate,
                             TupleTableSlot *searchslot)
```

## Detailed Description
This function provides a comprehensive tuple deletion workflow designed specifically for replication scenarios. It locates and deletes a tuple identified by the searchslot parameter while properly handling all associated database operations including trigger execution and replica identity validation.

The function follows a streamlined but complete workflow: first validating replica identity requirements for the DELETE operation, then executing BEFORE ROW DELETE triggers which may skip the deletion entirely. If the deletion proceeds, it performs the actual tuple deletion from the table and executes AFTER ROW DELETE triggers to maintain referential integrity and business logic.

Unlike the insert and update variants, this function is relatively simple since deletion doesn\t require constraint checking, index tuple creation, or stored generated column computation - it primarily focuses on trigger execution and the deletion operation itself. Index cleanup is handled automatically by the storage layer.