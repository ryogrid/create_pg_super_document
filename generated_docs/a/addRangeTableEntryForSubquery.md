# addRangeTableEntryForSubquery

## Location
src/backend/parser/parse_relation.c: 1638 - 1733

## Overview
Creates a range table entry for a subquery and adds it to the parser state, returning a ParseNamespaceItem with appropriate column type information and visibility settings.

## Definition


## Detailed Description
The  function creates a range table entry for subqueries appearing in FROM clauses, WITH clauses, or other contexts where a query result is treated as a relation. This function handles the complex process of:

1. Creating an RTE with type RTE_SUBQUERY
2. Managing column aliases - either from user-provided aliases or auto-generated from subquery target list
3. Extracting type information (data types, type modifiers, collations) from the subquery's target list
4. Validating that the number of specified aliases matches available columns
5. Setting visibility rules based on whether the subquery has a user-provided alias

Key behavior:
- If no alias is provided, creates an auto-generated name "unnamed_subquery" that is marked as not visible
- Non-visible subqueries only allow unqualified column references and won't conflict with other namespace entries
- No permission checking is performed on subqueries since they represent derived data
- Extracts column metadata from the subquery's target list, skipping junk columns

## Parameters / Member Variables
- : Parser state containing the range table and other parsing context
- : The Query node representing the subquery to be added as an RTE
- : Optional alias with column names; if NULL, auto-generates names from subquery
- : Boolean indicating whether this is a LATERAL subquery with access to preceding FROM items
- : Boolean indicating whether this entry originates from a FROM clause

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for RTE creation)
  - copyObject (for alias copying)
  - makeAlias (for auto-generated aliases)
  - makeString (for column name creation)
  - exprType, exprTypmod, exprCollation (type information extraction)
  - lappend, lappend_oid, lappend_int (list manipulation)
  - buildNSItemFromLists (namespace item creation)
  - ereport (error reporting)
- Called from (representative examples):
  - transformRangeSubselect (in parse_clause.c)
  - transformInsertStmt (in analyze.c)
  - transformSetOperationTree (in analyze.c)
  - convert_ANY_sublink_to_join (in subselect.c)

## Notes and Other Information
- Subqueries are never checked for access rights since they represent derived data, not base relations
- Column alias validation ensures the number of provided aliases matches the number of non-junk columns
- Auto-generated subquery names ("unnamed_subquery") are marked as not visible to prevent namespace conflicts
- The function carefully extracts type information from the subquery's target list to ensure proper column metadata
- LATERAL subqueries have special scoping rules allowing them to reference columns from preceding FROM items
- Error handling includes detailed messages when alias count mismatches occur