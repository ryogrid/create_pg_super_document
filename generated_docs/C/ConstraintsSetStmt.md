# ConstraintsSetStmt

## Location
[src/include/nodes/parsenodes.h:3954-3959](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3954-L3959)

## Overview
ConstraintsSetStmt represents a SET CONSTRAINTS statement in PostgreSQL's parse tree, which is used to change the checking mode of deferrable constraints within a transaction.

## Definition
```c
typedef struct ConstraintsSetStmt
{
    NodeTag     type;
    List       *constraints;  /* List of names as RangeVars */
    bool        deferred;
} ConstraintsSetStmt;
```

## Detailed Description
ConstraintsSetStmt is a parse tree node that represents the SET CONSTRAINTS SQL command. This statement allows users to control when deferrable constraints are checked within a transaction. Constraints can be set to IMMEDIATE (checked immediately when a statement completes) or DEFERRED (checked only at transaction commit).

The statement can target specific named constraints or use ALL to affect all deferrable constraints in the current transaction. This is particularly useful for complex data modifications where constraint violations might occur temporarily during intermediate steps but resolve by transaction end.

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as a ConstraintsSetStmt node in the parse tree
- `constraints`: List of RangeVar structures representing the constraint names to modify, or NIL for ALL constraints
- `deferred`: Boolean flag indicating the desired constraint checking mode (true = DEFERRED, false = IMMEDIATE)

## Dependencies
- Functions called/Symbols referenced:
  - [List](../L/List.md) (PostgreSQL's list data structure)
  - [RangeVar](../R/RangeVar.md) (for constraint name references)
  
- Called from (representative examples):
  - [AfterTriggerSetState](../A/AfterTriggerSetState.md) (main execution function in trigger.c:5746)
  - [PlannedStmtRequiresSnapshot](../P/PlannedStmtRequiresSnapshot.md) (snapshot requirement check in pquery.c:1743)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (utility command processor in utility.c:939)

## Notes and Other Information
- Only affects constraints that were declared as DEFERRABLE
- When constraints is NIL, the statement applies to ALL deferrable constraints in the transaction
- The deferred/immediate state is transaction-local and resets at transaction end
- Supports constraint name resolution across multiple schemas with specific precedence rules
- Handles partitioned tables by also affecting corresponding constraints in partitions
- The actual constraint state management is handled through PostgreSQL's after-trigger system
- Historical behavior prioritizes constraints from the first matching schema in the search path