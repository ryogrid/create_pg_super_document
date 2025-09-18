# TransformPubWhereClauses

## Location
[src/backend/commands/publicationcmds.c:605-676](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L605-L676)

## Overview
Transforms and validates WHERE clauses for all relations in a publication, ensuring they are properly coerced to boolean expressions with correct collation information.

## Definition
```c
static void TransformPubWhereClauses(List *tables, const char *queryString, bool pubviaroot)
```

## Detailed Description
This function processes publication WHERE clauses for multiple relations by performing several critical transformations and validations:

1. **Parse State Setup**: Creates a fresh ParseState for each relation with only that relation in its range table
2. **Namespace Management**: Adds a range table entry and namespace item for the relation
3. **WHERE Clause Transformation**: Uses transformWhereClause to properly parse and type-check the expression
4. **Collation Assignment**: Ensures proper collation information is assigned to the expression
5. **Validation**: Calls check_simple_rowfilter_expr to validate the expression meets publication requirements
6. **Partitioned Table Restrictions**: Enforces restrictions on WHERE clauses for partitioned tables when publish_via_partition_root is false

The function handles the complex interaction between PostgreSQL's parser infrastructure and publication-specific requirements, ensuring that WHERE clauses are both syntactically correct and semantically valid for logical replication.

## Parameters / Member Variables
- `tables`: List of PublicationRelInfo structures containing relations and their WHERE clauses
- `queryString`: Source text of the query for error reporting
- `pubviaroot`: Boolean indicating whether publication publishes via partition root

## Dependencies
- Functions called/Symbols referenced:
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md)
  - [PublicationRelInfo](../P/PublicationRelInfo.md)
  - [make_parsestate](../m/make_parsestate.md)
  - [addRangeTableEntryForRelation](../a/addRangeTableEntryForRelation.md)
  - [addNSItemToQuery](../a/addNSItemToQuery.md)
  - [transformWhereClause](../t/transformWhereClause.md)
  - copyObject
  - EXPR_KIND_WHERE
  - [assign_expr_collations](../a/assign_expr_collations.md)
  - [check_simple_rowfilter_expr](../c/check_simple_rowfilter_expr.md)
  - [free_parsestate](../f/free_parsestate.md)
- Called from:
  - [CreatePublication](../C/CreatePublication.md)
  - [AlterPublicationTables](../A/AlterPublicationTables.md)

## Notes and Other Information
- This is a static function used internally within publicationcmds.c
- The function modifies the whereClause field in each PublicationRelInfo structure
- Special handling for partitioned tables prevents WHERE clauses when publish_via_partition_root is false
- Each relation gets its own ParseState to ensure proper namespace isolation
- The transformed WHERE clauses are stored back in the PublicationRelInfo structures for later use
- Error messages provide specific context about publication WHERE clause restrictions