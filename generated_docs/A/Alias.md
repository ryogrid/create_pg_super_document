# Alias

## Location
[src/include/nodes/primnodes.h:47-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L47-L52)

## Overview
The Alias struct specifies an alias for a range variable and optionally provides renaming of columns within the table.

## Definition


## Detailed Description
The Alias struct is a fundamental node type in PostgreSQL's parser that represents table and column aliases in SQL queries. It serves as a container for alias information that can be applied to relations, subqueries, functions, and other range variables. The struct is designed to handle both simple table aliasing (AS alias_name) and column renaming scenarios (AS alias_name(col1, col2, ...)).

The aliasname field stores the primary alias for the relation and is never schema-qualified. The colnames field is an optional list of String nodes that provides column-level aliases. In Range Table Entries (RTEs), there may be entries in colnames corresponding to dropped columns, which are typically represented as empty strings.

## Parameters / Member Variables
- : Standard NodeTag for PostgreSQL's node system type identification
- : The primary alias name for the relation (never schema-qualified)
- : Optional list of String nodes representing column aliases; may contain empty strings for dropped columns

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (node system)
  - [List](../L/List.md) (PostgreSQL list structure)
  - String nodes (for column names)

- Called from (representative examples):
  - [makeAlias](../m/makeAlias.md) (creation function)
  - [addRangeTableEntry](../a/addRangeTableEntry.md) functions (various RTE creation)
  - [buildRelationAliases](../b/buildRelationAliases.md)
  - [expandRelation](../e/expandRelation.md)
  - [scanRTEForColumn](../s/scanRTEForColumn.md)
  - [transformJsonArrayQueryConstructor](../t/transformJsonArrayQueryConstructor.md)

## Notes and Other Information
- The Alias struct is used extensively throughout the parser for handling AS clauses in SQL
- Column aliases in the colnames list may include empty string entries for dropped columns
- The aliasname is always unqualified (no schema prefix)
- This structure is fundamental to PostgreSQL's range table entry (RTE) system
- Used in conjunction with RangeVar, RangeTblEntry, and other parser node types
- Critical for query transformation and column resolution processes