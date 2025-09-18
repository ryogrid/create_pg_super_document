# WindowDef

## Location
[src/include/nodes/parsenodes.h:561-572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L561-L572)

## Overview
WindowDef is a parse tree node that represents the raw representation of WINDOW and OVER clauses in SQL, containing all the specifications needed to define a window for window functions including partitioning, ordering, and framing.

## Definition


## Detailed Description
WindowDef nodes capture the complete specification of window definitions used in SQL window functions. They handle two main scenarios: entries in a WINDOW list where "name" defines a named window, and OVER clauses where "name" is used for "OVER window" syntax or "refname" for "OVER (window)" syntax. The latter case subtly differs by implying that the window frame clause can be overridden. The structure encompasses partitioning specifications (PARTITION BY), ordering specifications (ORDER BY), and frame boundary definitions (ROWS/RANGE clauses with optional offset expressions).

## Parameters / Member Variables
- : Standard NodeTag identifying this as a WindowDef node
- : The window's own name when defining a named window in a WINDOW clause
- : Referenced window name when using "OVER (window)" syntax, allowing inheritance with modifications
- : List of expressions for PARTITION BY clause, determining how to group rows
- : List of SortBy nodes for ORDER BY clause, specifying sort order within partitions
- : Integer bitfield containing frame clause options (ROWS/RANGE, UNBOUNDED/CURRENT/PRECEDING/FOLLOWING, etc.)
- : Expression defining the starting boundary offset for the window frame, if specified
- : Expression defining the ending boundary offset for the window frame, if specified
- : Parse location in the original query text, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (inherited structure member)
  - [List](../L/List.md) (for partition and order clauses)
  - [Node](../N/Node.md) (for offset expressions)
  - ParseLoc (for source location tracking)
  - [SortBy](../S/SortBy.md) (implicitly referenced in orderClause)
- Called from (representative examples):
  - [transformWindowDefinitions](../t/transformWindowDefinitions.md) (src/backend/parser/parse_clause.c:2775)
  - [transformWindowFuncCall](../t/transformWindowFuncCall.md) (src/backend/parser/parse_agg.c:821, 1008, 1030)
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md) (src/backend/parser/parse_func.c:96)

## Notes and Other Information
- [WindowDef](WindowDef.md) supports both named window definitions (WINDOW clause) and inline window specifications (OVER clause)
- The distinction between "name" and "refname" is crucial for proper window inheritance and frame clause overriding
- Frame options are encoded as bitfields to efficiently represent combinations of frame specifications
- Window definitions are later transformed into WindowClause nodes during query analysis
- Used extensively in window function processing and aggregate window operations
- File location: src/include/nodes/parsenodes.h:561-572