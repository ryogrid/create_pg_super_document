# ExecInitResultRelation

## Location
[src/backend/executor/execUtils.c:814-843](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L814-L843)

## Overview
Initializes a ResultRelInfo structure for a target relation in DML operations, opening the relation and setting up metadata needed for data modification.

## Definition
```c
void ExecInitResultRelation(EState *estate, ResultRelInfo *resultRelInfo, Index rti)
```

## Detailed Description
This function initializes a ResultRelInfo structure for a relation that will be the target of INSERT, UPDATE, or DELETE operations. It opens the relation using ExecGetRangeTableRelation, then calls InitResultRelInfo to populate the ResultRelInfo structure with necessary metadata including triggers, constraints, and partitioning information. The function also maintains two data structures in the execution state: es_result_relations array for direct access by range table index, and es_opened_result_relations list for efficient traversal of only the opened result relations during cleanup.

## Parameters / Member Variables
- `estate`: Execution state containing range table and result relation tracking structures
- `resultRelInfo`: ResultRelInfo structure to initialize (allocated by caller)
- `rti`: Range table index identifying which relation to initialize as a result relation

## Dependencies
- Functions called/Symbols referenced:
  - [ExecGetRangeTableRelation](ExecGetRangeTableRelation.md)
  - [InitResultRelInfo](../I/InitResultRelInfo.md)
  - [palloc0](../p/palloc0.md)
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md)
  - [ExecInitModifyTable](ExecInitModifyTable.md)

## Notes and Other Information
- Lazily allocates es_result_relations array when first result relation is initialized
- Maintains both array and list representations for different access patterns
- The ResultRelInfo structure contains metadata needed for constraint checking, trigger execution, and partition routing
- Used primarily in DML operations (INSERT, UPDATE, DELETE) and COPY operations