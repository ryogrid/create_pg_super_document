# ExecAuxRowMark

## Location
[src/include/nodes/execnodes.h:774-780](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L774-L780)

## Overview
ExecAuxRowMark provides additional runtime representation for FOR [KEY] UPDATE/SHARE clauses, extending ExecRowMark with resjunk column information.

## Definition

```c
typedef struct ExecAuxRowMark
{
	ExecRowMark *rowmark;		/* related entry in es_rowmarks */
	AttrNumber	ctidAttNo;		/* resno of ctid junk attribute, if any */
	AttrNumber	toidAttNo;		/* resno of tableoid junk attribute, if any */
	AttrNumber	wholeAttNo;		/* resno of whole-row junk attribute, if any */
} ExecAuxRowMark;
```
## Detailed Description
ExecAuxRowMark augments the basic ExecRowMark structure by providing attribute numbers for resjunk columns that are needed for row locking operations. These auxiliary structures are maintained by LockRows and ModifyTable nodes to efficiently access the special columns (ctid, tableoid, whole-row) that carry the information needed to locate and lock specific rows. The structure acts as a bridge between the general row marking information in ExecRowMark and the specific column positions in the result tuple.

## Parameters / Member Variables
- `*rowmark`: Pointer to the corresponding ExecRowMark entry in the EState's es_rowmarks array
- `ctidAttNo`: Attribute number (resno) of the ctid junk attribute in the result tuple, or InvalidAttrNumber if not present
- `toidAttNo`: Attribute number (resno) of the tableoid junk attribute in the result tuple, or InvalidAttrNumber if not present
- `wholeAttNo`: Attribute number (resno) of the whole-row junk attribute in the result tuple, or InvalidAttrNumber if not present
## Dependencies
- Functions called/Symbols referenced:
  - [ExecRowMark](ExecRowMark.md) (struct type)
- Called from (representative examples):
  - [ExecBuildAuxRowMark](ExecBuildAuxRowMark.md) (src/backend/executor/execMain.c:2404)
  - [ExecLockRows](ExecLockRows.md) (src/backend/executor/nodeLockRows.c:77)
  - [ExecInitModifyTable](ExecInitModifyTable.md) (src/backend/executor/nodeModifyTable.c:4796)

## Notes and Other Information
ExecAuxRowMark is essential for the efficient implementation of row locking in complex queries. The resjunk columns it references contain the metadata needed to locate and lock rows: ctid provides the physical row location, tableoid identifies the specific table in inheritance hierarchies, and whole-row attributes contain complete row data when needed. This design allows the executor to quickly extract locking-related information from result tuples without scanning for column positions repeatedly.