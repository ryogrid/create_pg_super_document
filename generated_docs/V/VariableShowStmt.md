# VariableShowStmt

## Location
[src/include/nodes/parsenodes.h:2631-2635](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2631-L2635)

## Overview
VariableShowStmt is a parse tree node structure that represents a SHOW statement used to display configuration parameter values or runtime information in PostgreSQL.

## Definition
```c
typedef struct VariableShowStmt
{
    NodeTag     type;
    char       *name;
} VariableShowStmt;
```

## Detailed Description
VariableShowStmt is a simple structure that holds the parsed representation of SHOW commands in PostgreSQL. The SHOW statement is used to display the current value of a configuration parameter (like `SHOW work_mem`) or runtime information (like `SHOW ALL`). This structure is created during SQL parsing and is part of the query tree that gets processed by the utility command handler.

The structure follows PostgreSQL's standard node pattern with a NodeTag for type identification and runtime type checking. The name field stores the parameter name to be shown, or special values like "ALL" for showing all parameters.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a VariableShowStmt node type
- `name`: String containing the name of the configuration parameter to show, or special keywords like "ALL"

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (inherited structure member)
  
- Called from (representative examples):
  - exec_replication_command (src/backend/replication/walsender.c:2180)
  - PlannedStmtRequiresSnapshot (src/backend/tcop/pquery.c:1742)  
  - standard_ProcessUtility (src/backend/tcop/utility.c:877)
  - UtilityTupleDescriptor (src/backend/tcop/utility.c:2118)

## Notes and Other Information
- This structure is part of PostgreSQL's parse tree node hierarchy defined in parsenodes.h
- The actual processing of SHOW commands happens in the utility command processing infrastructure
- Common SHOW commands include `SHOW work_mem`, `SHOW ALL`, `SHOW search_path`, etc.
- The structure is lightweight as it only needs to carry the parameter name for later processing