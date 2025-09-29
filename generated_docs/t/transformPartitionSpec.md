# transformPartitionSpec

## Location
[src/backend/commands/tablecmds.c:17988-18045](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L17988-L18045)

## Overview
Transforms and validates partition key expressions in a PartitionSpec by parsing expressions, checking strategy constraints, and assigning proper collations for table partitioning operations.

## Definition
```c
static PartitionSpec *transformPartitionSpec(Relation rel, PartitionSpec *partspec)
```

## Detailed Description
This function processes partition specifications during table creation or alteration by transforming any expressions present in partition keys. It validates partitioning strategy constraints, particularly enforcing that LIST partitioning can only use a single column, and creates a properly transformed PartitionSpec with resolved expressions and collations.

The function creates a dummy ParseState to provide the necessary context for expression transformation, adding the target relation to the range table. It then iterates through each partition parameter, transforming any expressions using PostgreSQL's standard expression parser and assigning appropriate collations to ensure proper comparison semantics for partition key evaluation.

## Parameters / Member Variables
- `rel`: Relation object representing the table being partitioned
- `partspec`: Input PartitionSpec containing the raw partition specification with potentially untransformed expressions

## Dependencies
- Functions called/Symbols referenced:
  - makeNode - Creates new PartitionSpec node
  - [make_parsestate](../m/make_parsestate.md) - Creates parser state for expression transformation
  - [addRangeTableEntryForRelation](../a/addRangeTableEntryForRelation.md) - Adds relation to parser's range table
  - [addNSItemToQuery](../a/addNSItemToQuery.md) - Adds namespace item to query context
  - lfirst_node - [List](../L/List.md) iteration macro for PartitionElem nodes
  - copyObject - Deep copies partition element to avoid modifying input
  - [transformExpr](transformExpr.md) - Transforms expressions using parser context
  - [assign_expr_collations](../a/assign_expr_collations.md) - Assigns collations to transformed expressions
  - [lappend](../l/lappend.md) - Appends elements to result list
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md) (src/backend/commands/tablecmds.c:1170)

## Notes and Other Information
- Static function scope limits visibility to tablecmds.c module
- Enforces LIST partitioning constraint of single column only
- Creates dummy ParseState with AccessShareLock on the target relation
- Uses EXPR_KIND_PARTITION_EXPRESSION for proper expression context during transformation
- Preserves original partition strategy and location information
- Deep copies partition elements to avoid modifying input specification
- Essential for proper partition constraint evaluation and partition pruning
- Part of the table command infrastructure in src/backend/commands/tablecmds.c (lines 17988-18045)

## Simplified Source

```c
static PartitionSpec *
transformPartitionSpec(Relation rel, PartitionSpec *partspec)
{
    PartitionSpec *newspec;
    ParseState *pstate;
    ParseNamespaceItem *nsitem;
    ListCell *l;

    // Create new PartitionSpec with basic info copied
    newspec = makeNode(PartitionSpec);
    newspec->strategy = partspec->strategy;
    newspec->partParams = NIL;
    newspec->location = partspec->location;

    // Validate strategy constraints - LIST requires single column
    if (partspec->strategy == PARTITION_STRATEGY_LIST &&
        list_length(partspec->partParams) != 1)
        ereport(ERROR, "list partition strategy requires exactly one column");

    // Create parser context for expression transformation
    pstate = make_parsestate(NULL);
    nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock,
                                          NULL, false, true);
    addNSItemToQuery(pstate, nsitem, true, true, true);

    // Transform each partition parameter
    foreach(l, partspec->partParams) {
        PartitionElem *pelem = lfirst_node(PartitionElem, l);

        // Transform expressions if present
        if (pelem->expr) {
            // Copy to avoid modifying input
            pelem = copyObject(pelem);

            // Transform expression with proper context
            pelem->expr = transformExpr(pstate, pelem->expr,
                                      EXPR_KIND_PARTITION_EXPRESSION);

            // Assign proper collations for comparison
            assign_expr_collations(pstate, pelem->expr);
        }

        // Add transformed element to result
        newspec->partParams = lappend(newspec->partParams, pelem);
    }

    return newspec;
}
```