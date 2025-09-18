# addRangeTableEntryForJoin

## Location
[src/backend/parser/parse_relation.c:2216-2313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L2216-L2313)

## Overview
Creates a range table entry (RTE) for a join operation and adds it to the parser state's range table, returning a ParseNamespaceItem for the new join entry.

## Definition
```c
ParseNamespaceItem *addRangeTableEntryForJoin(ParseState *pstate,
                                              List *colnames,
                                              ParseNamespaceColumn *nscolumns,
                                              JoinType jointype,
                                              int nummergedcols,
                                              List *aliasvars,
                                              List *leftcols,
                                              List *rightcols,
                                              Alias *join_using_alias,
                                              Alias *alias,
                                              bool inFromCl)
```

## Detailed Description
This function creates a RangeTblEntry of type RTE_JOIN for handling join operations in SQL statements. It constructs the RTE with comprehensive join metadata including column mappings, join type information, and alias management. The function validates that the join doesn't exceed PostgreSQL's maximum column limit (MaxAttrNumber) and handles alias column name resolution. Unlike other RTE creation functions, this one allows the caller to provide a pre-constructed ParseNamespaceColumn array for more precise namespace control. The function sets lateral to false as joins are not lateral by default.

## Parameters / Member Variables
- `pstate`: ParseState containing the current parsing context and range table
- `colnames`: List of column names for the join result
- `nscolumns`: Pre-constructed ParseNamespaceColumn array for namespace management
- `jointype`: Type of join operation (INNER, LEFT, RIGHT, FULL, etc.)
- `nummergedcols`: Number of columns that are merged in the join
- `aliasvars`: List of alias variables for the join columns
- `leftcols`: List of column references from the left side of the join
- `rightcols`: List of column references from the right side of the join
- `join_using_alias`: Alias for USING clause columns if applicable
- `alias`: Optional alias for the entire join (uses "unnamed_join" if NULL)
- `inFromCl`: Boolean indicating if this appears in the FROM clause

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for RangeTblEntry creation)
  - copyObject (for alias copying)
  - [makeAlias](../m/makeAlias.md) (for default alias creation)
  - [list_concat](../l/list_concat.md) (for combining column name lists)
  - [list_copy_tail](../l/list_copy_tail.md) (for copying partial lists)
  - [palloc](../p/palloc.md) (for ParseNamespaceItem allocation)
- Called from (representative examples):
  - [transformSetOperationStmt](../t/transformSetOperationStmt.md) (in analyze.c:1871)
  - [transformFromClauseItem](../t/transformFromClauseItem.md) (in parse_clause.c:1543)

## Notes and Other Information
- Enforces PostgreSQL's limit of MaxAttrNumber columns per join
- Access permissions are not checked for join RTEs as they inherit permissions from their constituent relations
- The function automatically fills in missing alias column names from the provided colnames list
- Sets default visibility flags for the namespace item which may be modified later by the caller
- Joins are never lateral references by default (lateral = false)
- Located in src/backend/parser/parse_relation.c:2216-2313