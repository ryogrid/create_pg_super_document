# CollateClause

## Location
[src/include/nodes/parsenodes.h:381-387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L381-L387)

## Overview
CollateClause represents a COLLATE expression in PostgreSQL's parse tree, used to specify collation rules for character data comparison and sorting operations.

## Definition


## Detailed Description
CollateClause is a parse tree node that represents collation specifications in SQL statements. Collation determines how text values are compared and sorted, including rules for case sensitivity, accent sensitivity, and locale-specific ordering. This node encapsulates an expression along with the specified collation name, which can be schema-qualified. It's commonly used in column definitions, domain definitions, and expressions where specific text comparison behavior is required.

## Parameters / Member Variables
- : NodeTag identifying this as a CollateClause node
- : Pointer to the Node representing the input expression to which collation is applied
- : List containing the possibly schema-qualified collation name (e.g., "en_US.utf8" or "schema.collation_name")
- : ParseLoc storing the token's position in the source SQL, or -1 if location is unknown

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc
  - [Node](../N/Node.md) (generic parse tree node)
  - [List](../L/List.md) (PostgreSQL list structure)
- Called from (representative examples):
  - [transformCollateClause](../t/transformCollateClause.md)
  - [transformExprRecurse](../t/transformExprRecurse.md)
  - [FigureColnameInternal](../F/FigureColnameInternal.md)
  - [ColumnDef](ColumnDef.md) (in column definitions)
  - CreateDomainStmt (in domain definitions)

## Notes and Other Information
- [CollateClause](CollateClause.md) nodes are transformed during analysis to apply the specified collation to expressions
- The collname List typically contains String nodes representing the components of the collation name
- Used extensively in internationalization scenarios where locale-specific text handling is required
- Can be applied to column definitions, domain definitions, and individual expressions
- The specified collation must be available in the database system
- Location information helps provide accurate error messages for invalid collation specifications