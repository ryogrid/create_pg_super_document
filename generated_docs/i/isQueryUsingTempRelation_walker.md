# isQueryUsingTempRelation_walker

## Location
[src/backend/parser/parse_relation.c:3830-3873](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3830-L3873)

## Overview
This is a static helper function that performs the actual tree traversal to detect temporary relations within a query structure using the walker pattern.

## Definition
```c
static bool isQueryUsingTempRelation_walker(Node *node, void *context)
```

## Detailed Description
`isQueryUsingTempRelation_walker` implements the core logic for detecting temporary relations within a query tree. It uses PostgreSQL's tree walker pattern to recursively traverse query structures and expression trees. When it encounters a Query node, it examines the range table (rtable) for any RTE_RELATION entries and checks their persistence characteristics by opening the relation and inspecting the `relpersistence` field. If any relation has `RELPERSISTENCE_TEMP`, the function returns true. The function recursively calls itself through `query_tree_walker` for nested queries and `expression_tree_walker` for expression nodes, ensuring comprehensive coverage of the entire query structure.

## Parameters / Member Variables
- `node`: Pointer to a Node in the query/expression tree being examined
- `context`: Unused context parameter (passed as NULL), maintained for walker function signature compatibility

## Dependencies
- Functions called/Symbols referenced:
  - query_tree_walker
  - expression_tree_walker
  - [table_open](../t/table_open.md)
  - [table_close](../t/table_close.md)
  - lfirst
- Constants/Enums referenced:
  - RTE_RELATION
  - RELPERSISTENCE_TEMP
  - [QTW_IGNORE_JOINALIASES](../Q/QTW_IGNORE_JOINALIASES.md)
  - AccessShareLock
- Called from (representative examples):
  - [isQueryUsingTempRelation](isQueryUsingTempRelation.md)
  - [isQueryUsingTempRelation_walker](isQueryUsingTempRelation_walker.md) (recursive calls)

## Notes and Other Information
- This is a static function, not part of the public interface
- Uses the standard PostgreSQL walker pattern with recursive traversal
- Acquires AccessShareLock when opening relations to check their persistence, ensuring safe access to relation metadata
- The QTW_IGNORE_JOINALIASES flag is used to avoid examining join alias structures during tree walking
- The function performs early termination - returns true immediately upon finding the first temporary relation
- Memory management is handled by the walker framework and relation opening/closing functions

## Simplified Source
```c
static bool
isQueryUsingTempRelation_walker(Node *node, void *context)
{
    if (node == NULL)
        return false;

    if (IsA(node, Query))
    {
        Query *query = (Query *) node;

        // Check each relation in the range table
        foreach(rtable, query->rtable)
        {
            RangeTblEntry *rte = lfirst(rtable);

            if (rte->rtekind == RTE_RELATION)
            {
                // Open relation to check if it's temporary
                Relation rel = table_open(rte->relid, AccessShareLock);
                char persistence = rel->rd_rel->relpersistence;
                table_close(rel, AccessShareLock);

                if (persistence == RELPERSISTENCE_TEMP)
                    return true;  // Found temporary relation
            }
        }

        // Recursively check nested queries
        return query_tree_walker(query, isQueryUsingTempRelation_walker,
                                context, QTW_IGNORE_JOINALIASES);
    }

    // Recursively check expression nodes
    return expression_tree_walker(node, isQueryUsingTempRelation_walker, context);
}
```