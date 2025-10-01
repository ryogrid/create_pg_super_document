# makeJsonTableSiblingJoin

## Location
[src/backend/parser/parse_jsontable.c:534-543](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_jsontable.c#L534-L543)

## Overview
Creates a JsonTableSiblingJoin plan node that performs a UNION operation between rows from two sibling JSON table plans.

## Definition
```c
static JsonTablePlan *makeJsonTableSiblingJoin(JsonTablePlan *lplan, JsonTablePlan *rplan)
```

## Detailed Description
This static function constructs a JsonTableSiblingJoin execution plan node that combines results from two sibling JSON table plans using UNION semantics. The sibling join represents the SQL/JSON standard behavior for combining rows from multiple NESTED COLUMNS clauses within the same JSON_TABLE specification.

The function creates a binary tree structure where the left and right child plans represent independent nested column specifications. During execution, the sibling join will produce all rows from the left plan followed by all rows from the right plan, effectively implementing a UNION ALL operation.

This construct is essential for handling JSON_TABLE expressions that contain multiple NESTED COLUMNS clauses at the same nesting level, allowing each nested specification to contribute its rows to the final result set independently.

## Parameters / Member Variables
- `lplan`: Left JsonTablePlan representing the first sibling nested column specification
- `rplan`: Right JsonTablePlan representing the second sibling nested column specification

## Dependencies
- Functions called/Symbols referenced:
  - makeNode: Creates new PostgreSQL parse tree nodes
  - T_JsonTableSiblingJoin: Node type identifier for sibling join plans
  - [JsonTableSiblingJoin](../J/JsonTableSiblingJoin.md): Structure type for sibling join plan nodes

- Called from (representative examples):
  - [transformJsonTableNestedColumns](../t/transformJsonTableNestedColumns.md): Used when combining multiple nested column specifications

## Notes and Other Information
- The join semantics follow UNION ALL behavior (no duplicate elimination)
- Sibling joins can be nested to handle arbitrary numbers of nested column specifications
- The plan structure supports the SQL/JSON standard requirement for independent nested column evaluation
- Left and right plan ordering preserves the textual order of NESTED COLUMNS clauses in the original query

## Simplified Source

```c
static JsonTablePlan *makeJsonTableSiblingJoin(JsonTablePlan *lplan, JsonTablePlan *rplan) {
    // Create new sibling join node
    JsonTableSiblingJoin *join = makeNode(JsonTableSiblingJoin);

    // Initialize join plan fields
    join->plan.type = T_JsonTableSiblingJoin;
    join->lplan = lplan;
    join->rplan = rplan;

    return (JsonTablePlan *) join;
}
```