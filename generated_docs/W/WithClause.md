# WithClause

## Location
src/include/nodes/parsenodes.h: 1592 - 1598

## Overview
WithClause represents the parser's representation of WITH clauses (Common Table Expressions), containing a list of CTEs and metadata about whether the clause is recursive.

## Definition

```c
typedef struct WithClause
{
	NodeTag		type;
	List	   *ctes;			/* list of CommonTableExprs */
	bool		recursive;		/* true = WITH RECURSIVE */
	ParseLoc	location;		/* token location, or -1 if unknown */
} WithClause;
```
## Detailed Description
WithClause serves as a container for Common Table Expressions (CTEs) during the parsing phase. It captures both regular WITH clauses and WITH RECURSIVE clauses, storing the list of individual CTEs along with the recursive flag that affects the processing semantics of the entire clause.

This structure exists only during parsing and analysis phases. While WithClause itself does not propagate into the final Query representation, the individual CommonTableExpr nodes it contains are transferred to the Query's cteList. This design separates the syntactic grouping of CTEs from their semantic representation in the query tree.

The recursive flag applies to the entire WITH clause and enables different processing rules during analysis, particularly for dependency checking and recursion validation. When true, it allows CTEs within the clause to reference each other, including self-references for recursive queries.

## Parameters / Member Variables
- : NodeTag identifying this as a WithClause node
- : List of CommonTableExpr nodes representing the individual CTEs in the WITH clause
- : Boolean flag indicating whether this is a WITH RECURSIVE clause
- : ParseLoc indicating the source location of the WITH keyword for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - CommonTableExpr (individual CTE nodes)
  - List (for CTE storage)
  - ParseLoc (for source location tracking)
- Called from (representative examples):
  - transformWithClause (parser/parse_cte.c)
  - makeDependencyGraphWalker (parser/parse_cte.c)
  - checkWellFormedRecursionWalker (parser/parse_cte.c)
  - transformSetOperationStmt (parser/analyze.c)

## Notes and Other Information
- WithClause is a parsing-phase structure that does not appear in the final Query representation
- The recursive flag affects the entire WITH clause, not individual CTEs
- Individual CommonTableExpr nodes from the ctes list are moved to Query.cteList during analysis
- WITH RECURSIVE enables CTEs to reference each other and themselves for recursive queries
- The structure is used in multiple statement types: SELECT, INSERT, UPDATE, DELETE, and MERGE
- Location information supports better error reporting during parsing and analysis
- Dependency analysis and recursion validation are performed using walker functions that operate on WithClause structures