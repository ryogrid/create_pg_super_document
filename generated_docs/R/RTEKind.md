# RTEKind

## Location
src/include/nodes/parsenodes.h: 1036 - 1037

## Overview
RTEKind is an enumeration that defines the different types of range table entries that can appear in PostgreSQL's query processing, indicating what kind of relation or data source each entry represents.

## Definition


## Detailed Description
RTEKind is used to classify range table entries (RTEs) in PostgreSQL's query tree structure. Each value represents a different kind of data source that can appear in a query's FROM clause or be referenced during query processing. The enumeration helps the query planner and executor determine how to handle each entry in the range table, as different types require different processing strategies.

## Parameters / Member Variables
- : Represents an ordinary table, view, or any relation with a pg_class entry
- : Represents a subquery appearing in the FROM clause
- : Represents an explicit JOIN operation result
- : Represents a function call appearing in the FROM clause
- : Represents table functions with explicit column lists
- : Represents VALUES clauses with multiple expression lists
- : Represents Common Table Expression (WITH clause) references
- : Represents named tuple stores, often used for triggers
- : Represents empty FROM clauses, added by the planner during optimization

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enumeration type)
- Called from (representative examples):
  - _outRangeTblEntry (src/backend/nodes/outfuncs.c:502)
  - _readRangeTblEntry (src/backend/nodes/readfuncs.c:353)
  - RangeTblEntry (src/include/nodes/parsenodes.h:1054)
  - RelOptInfo (src/include/nodes/pathnodes.h:916)

## Notes and Other Information
- RTEKind is fundamental to PostgreSQL's query processing architecture
- The RTE_RESULT type is specifically added by the planner and not present during initial parsing
- Different RTE types require different handling strategies in query planning and execution
- The enumeration is part of the parse tree node structure defined in parsenodes.h