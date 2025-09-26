# JsonObjectAgg

## Location
[src/include/nodes/parsenodes.h:1976-1983](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1976-L1983)

## Overview
JsonObjectAgg represents the untransformed (parse tree) representation of the JSON_OBJECTAGG() aggregate function, which constructs JSON objects from key-value pairs.

## Definition
```c
typedef struct JsonObjectAgg
{
    NodeTag             type;
    JsonAggConstructor *constructor;    /* common fields */
    JsonKeyValue       *arg;            /* object key-value pair */
    bool                absent_on_null; /* skip NULL values? */
    bool                unique;         /* check key uniqueness? */
} JsonObjectAgg;
```

## Detailed Description
JsonObjectAgg represents the parsed form of JSON_OBJECTAGG() function calls before transformation into executable form. This aggregate function builds JSON objects by aggregating key-value pairs. The structure includes common aggregate functionality through the JsonAggConstructor, the key-value specification, and behavioral flags for handling NULL values and key uniqueness constraints.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a JsonObjectAgg node
- `constructor`: Pointer to JsonAggConstructor containing common aggregate fields (output format, filter, ordering, window specification)
- `arg`: Pointer to JsonKeyValue structure specifying the key-value pair to aggregate
- `absent_on_null`: Boolean flag indicating whether to skip entries when values are NULL
- `unique`: Boolean flag indicating whether to enforce key uniqueness during aggregation

## Dependencies
- Functions called/Symbols referenced:
  - [JsonAggConstructor](JsonAggConstructor.md)
  - [JsonKeyValue](JsonKeyValue.md)
- Called from (representative examples):
  - [exprLocation](../e/exprLocation.md)
  - [transformExprRecurse](../t/transformExprRecurse.md)
  - [transformJsonObjectAgg](../t/transformJsonObjectAgg.md)
  - [raw_expression_tree_walker_impl](../r/raw_expression_tree_walker_impl.md)

## Notes and Other Information
- This structure is used during the parsing phase before transformation into execution-ready aggregate expressions
- The absent_on_null flag controls JSON object construction behavior when encountering NULL values
- The unique flag enables duplicate key detection and error handling during aggregation
- Inherits all standard aggregate features (filtering, ordering, windowing) through the constructor field
- Part of PostgreSQL's JSON aggregate function support introduced for SQL/JSON standard compliance