# isQueryUsingTempRelation

## Location
[src/backend/parser/parse_relation.c:3824-3829](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3824-L3829)

## Overview
This function examines a fully-parsed query and returns true if any relation underlying the query is a temporary relation (table, view, or materialized view).

## Definition
```c
bool isQueryUsingTempRelation(Query *query)
```

## Detailed Description
`isQueryUsingTempRelation` serves as a public interface to determine whether a given query involves any temporary relations. It acts as a wrapper function that delegates the actual traversal and checking logic to `isQueryUsingTempRelation_walker`. This function is primarily used in scenarios where the system needs to determine if a query should be treated differently due to the involvement of temporary objects, such as in view definitions or CREATE TABLE AS statements where temporary relations might affect the persistence characteristics of the resulting object.

## Parameters / Member Variables
- `query`: Pointer to a Query structure representing the fully-parsed query to be examined

## Dependencies
- Functions called/Symbols referenced:
  - [isQueryUsingTempRelation_walker](isQueryUsingTempRelation_walker.md)
- Called from (representative examples):
  - [DefineView](../D/DefineView.md) (src/backend/commands/view.c:487)
  - [transformCreateTableAsStmt](../t/transformCreateTableAsStmt.md) (src/backend/parser/analyze.c:3040)

## Notes and Other Information
- This function is declared in src/include/parser/parse_relation.h and is part of the public parser interface
- The function uses a tree walker pattern to recursively examine all parts of the query structure
- Temporary relation detection is based on checking the `relpersistence` field of relations, specifically looking for `RELPERSISTENCE_TEMP`
- The function is used to ensure that operations involving temporary relations are handled appropriately, particularly in contexts where the temporary nature of relations affects the operation's semantics

## Simplified Source

```c
bool
isQueryUsingTempRelation(Query *query)
{
    // Delegate to walker function for recursive traversal
    return isQueryUsingTempRelation_walker((Node *) query, NULL);
}
```