# SetOpCmd

## Location
src/include/nodes/nodes.h: 401 - 402

## Overview
SetOpCmd is an enumeration that defines the type of set operation commands for SetOp plan nodes in PostgreSQL's query execution engine.

## Definition


## Detailed Description
SetOpCmd specifies the semantics of set operations in PostgreSQL queries. It distinguishes between INTERSECT and EXCEPT operations, as well as their ALL variants. This enumeration is used by the query planner and executor to determine the appropriate behavior for set operations between query results. The enum is defined in nodes.h because it's needed in both pathnodes.h and plannodes.h for path generation and plan node execution.

## Parameters / Member Variables
- : Standard INTERSECT operation that returns rows present in both input sets, removing duplicates
- : INTERSECT ALL operation that returns rows present in both input sets, preserving duplicates  
- : Standard EXCEPT operation that returns rows from the first set that are not in the second set, removing duplicates
- : EXCEPT ALL operation that returns rows from the first set that are not in the second set, preserving duplicates

## Dependencies
- Functions called/Symbols referenced:
  - (This is an enum type with no function calls)
- Called from (representative examples):
  - [make_setop](../m/make_setop.md) (src/backend/optimizer/plan/createplan.c:6884)
  - [generate_nonunion_paths](../g/generate_nonunion_paths.md) (src/backend/optimizer/prep/prepunion.c:1042)
  - [create_setop_path](../c/create_setop_path.md) (src/backend/optimizer/util/pathnode.c:3558)
  - SetOpPath (src/include/nodes/pathnodes.h:2336)
  - SetOp (src/include/nodes/plannodes.h:1222)

## Notes and Other Information
This enumeration works in conjunction with SetOpStrategy to provide complete specification of set operation execution. The placement in nodes.h reflects its fundamental role in the query planning and execution infrastructure, being shared across multiple subsystems of the PostgreSQL query processor.