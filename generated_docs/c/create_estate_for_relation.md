# create_estate_for_relation

## Location
[src/backend/replication/pgoutput/pgoutput.c:820-849](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L820-L849)

## Overview
Creates and initializes an executor state (EState) for evaluating row filter expressions on a specific relation in logical replication.

## Definition


## Detailed Description
This function prepares the PostgreSQL executor infrastructure needed to evaluate row filter expressions for logical replication. It creates an EState object, constructs a range table entry (RTE) for the specified relation, and initializes the executor's range table with the relation information. The function sets up the necessary metadata including relation ID, kind, and lock mode (AccessShareLock), and configures permission information. The resulting EState can then be used to execute filter expressions against tuples from the relation.

## Parameters / Member Variables
- : Relation representing the table for which the executor state is being created

## Dependencies
- Functions called/Symbols referenced:
  - [CreateExecutorState](../C/CreateExecutorState.md)
  - makeNode (macro for RangeTblEntry)
  - RelationGetRelid
  - [addRTEPermissionInfo](../a/addRTEPermissionInfo.md)
  - ExecInitRangeTable
  - list_make1
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md)
  - RTE_RELATION (constant)
  - AccessShareLock (constant)
  - [EState](../E/EState.md) (return type)
  - [RangeTblEntry](../R/RangeTblEntry.md) (type)
  - [List](../L/List.md) (type)
- Called from (representative examples):
  - [pgoutput_row_filter_init](../p/pgoutput_row_filter_init.md)

## Notes and Other Information
This function is part of PostgreSQL's logical replication row filtering infrastructure. The executor state it creates provides the necessary context for evaluating WHERE-clause-like expressions that determine which rows should be replicated. The use of AccessShareLock ensures that the relation structure remains stable during filter evaluation without blocking concurrent operations. The function sets the command ID to the current command, which is important for visibility and transaction isolation when evaluating expressions.