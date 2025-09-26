# pgstat_unlink_relation

## Location
[src/backend/utils/activity/pgstat_relation.c:153-168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L153-L168)

## Overview
Breaks the bidirectional link between a relation cache entry and its associated statistics entry, ensuring clean disconnection when either end of the relationship is being removed.

## Definition
```c
void pgstat_unlink_relation(Relation rel)
```

## Detailed Description
This function safely disconnects a relation from its statistics tracking infrastructure by breaking the mutual references between the relation cache entry and the pending statistics entry. The function is designed to be called whenever either the relation or the statistics entry is being removed or when statistics tracking is being disabled.

The function performs a sanity check to ensure the bidirectional link is consistent before breaking it. If the relation has an associated statistics entry, it verifies that the statistics entry correctly points back to the relation, then sets both pointers to NULL to cleanly break the association.

This is a critical cleanup function that prevents dangling pointers and ensures that neither the relation cache system nor the statistics system holds invalid references after one side of the relationship is destroyed.

## Parameters / Member Variables
- `rel`: The Relation object whose statistics association should be removed

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a simple cleanup function)
- Called from (representative examples):
  - pgstat_init_relation (when disabling statistics)
  - RelationDestroyRelation (during relation cleanup)
  - pgstat_relation_delete_pending_cb (during statistics entry cleanup)

## Notes and Other Information
- This function is safe to call multiple times or on relations that have no statistics association
- The function includes an assertion to verify the integrity of the bidirectional link before breaking it
- It is called both when relations are destroyed and when statistics tracking is disabled dynamically
- The function ensures that no dangling pointers remain in either the relation cache or statistics system
- This is part of the cleanup path for both normal relation closure and error recovery scenarios
- The function is designed to be idempotent - calling it multiple times has no additional effect