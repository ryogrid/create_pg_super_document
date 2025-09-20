# ParseNamespaceItem

## Location
[src/include/parser/parse_node.h:284-318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/parser/parse_node.h#L284-L318)

## Overview
ParseNamespaceItem represents an element in the parser's namespace list, defining how table and column names are visible and accessible during SQL parsing operations.

## Definition

```c
struct ParseNamespaceItem
{
	Alias	   *p_names;		/* Table and column names */
	RangeTblEntry *p_rte;		/* The relation's rangetable entry */
	int			p_rtindex;		/* The relation's index in the rangetable */
	RTEPermissionInfo *p_perminfo;	/* The relation's rteperminfos entry */
	/* array of same length as p_names->colnames: */
	ParseNamespaceColumn *p_nscolumns;	/* per-column data */
	bool		p_rel_visible;	/* Relation name is visible? */
	bool		p_cols_visible; /* Column names visible as unqualified refs? */
	bool		p_lateral_only; /* Is only visible to LATERAL expressions? */
	bool		p_lateral_ok;	/* If so, does join type allow use? */
};
```
## Detailed Description
ParseNamespaceItem is a crucial structure in PostgreSQL's name resolution system during SQL parsing. It represents a single entry in the parser's namespace that determines how table and column names can be referenced in queries. The structure handles complex visibility rules that distinguish between qualified and unqualified name references, implements LATERAL visibility semantics, and manages the relationship between namespace items and the underlying range table entries.

The visibility flags (p_rel_visible and p_cols_visible) implement SQL's complex scoping rules where, for example, a JOIN without an alias makes the joined tables visible for qualified references but hides their individual columns for unqualified references. This prevents ambiguity while maintaining SQL standard compliance.

## Parameters / Member Variables
- `*p_names`: Alias structure containing table name and column names exposed by this namespace item
- `*p_rte`: Pointer to the underlying RangeTblEntry that this namespace item represents
- `p_rtindex`: Index position of the relation in the range table for quick lookup
- `*p_perminfo`: Pointer to permission information entry for the relation
- `*p_nscolumns`: Array of per-column information, parallel to p_names->colnames
- `p_rel_visible`: Whether the relation name can be used in qualified references
- `p_cols_visible`: Whether column names are accessible via unqualified references
- `p_lateral_only`: Whether this item is only visible to LATERAL subexpressions
- `p_lateral_ok`: Whether the join type permits actual use of LATERAL references

## Dependencies
- Functions called/Symbols referenced:
  - [Alias](../A/Alias.md) (for p_names field)
  - [RangeTblEntry](../R/RangeTblEntry.md) (for p_rte field)
  - [RTEPermissionInfo](../R/RTEPermissionInfo.md) (for p_perminfo field)
  - [ParseNamespaceColumn](ParseNamespaceColumn.md) (for p_nscolumns array)

- Called from (representative examples):
  - [buildNSItemFromTupleDesc](../b/buildNSItemFromTupleDesc.md)
  - [buildNSItemFromLists](../b/buildNSItemFromLists.md)
  - [addRangeTableEntry](../a/addRangeTableEntry.md) functions
  - [scanNameSpaceForRefname](../s/scanNameSpaceForRefname.md)
  - [refnameNamespaceItem](../r/refnameNamespaceItem.md)
  - [transformFromClauseItem](../t/transformFromClauseItem.md)
  - [setNamespaceColumnVisibility](../s/setNamespaceColumnVisibility.md)

## Notes and Other Information
ParseNamespaceItem implements sophisticated visibility semantics required by SQL standards. The separation of p_rel_visible and p_cols_visible flags handles cases like:

1. JOINs without aliases: Tables remain visible for qualified access (table.column) but columns are hidden for unqualified access to prevent ambiguity
2. Subqueries without aliases: Columns remain visible for unqualified access but the auto-generated relation name is hidden
3. Special constructs like NEW/OLD in rules may have only one visibility flag set

The LATERAL-related flags (p_lateral_only, p_lateral_ok) implement SQL:2008 LATERAL semantics, ensuring proper scoping of lateral references while providing clear error messages when lateral references are used incorrectly.

The p_nscolumns array provides detailed per-column information needed for constructing proper Var nodes during name resolution, including handling of dropped columns and type information.