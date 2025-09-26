# JsonArrayAgg

## Location
[src/include/nodes/parsenodes.h:1989-1995](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1989-L1995)

## Overview
JsonArrayAgg represents the untransformed (parse tree) representation of the JSON_ARRAYAGG() aggregate function, which constructs JSON arrays from aggregated values.

## Definition
```c
typedef struct JsonArrayAgg
{
    NodeTag             type;
    JsonAggConstructor *constructor;    /* common fields */
    JsonValueExpr      *arg;            /* array element expression */
    bool                absent_on_null; /* skip NULL elements? */
} JsonArrayAgg;
```

## Detailed Description
JsonArrayAgg represents the parsed form of JSON_ARRAYAGG() function calls before transformation into executable form. This aggregate function builds JSON arrays by collecting values from multiple rows. The structure includes common aggregate functionality through the JsonAggConstructor, the value expression to aggregate, and a behavioral flag for handling NULL values.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a JsonArrayAgg node
- `constructor`: Pointer to JsonAggConstructor containing common aggregate fields (output format, filter, ordering, window specification)
- `arg`: Pointer to JsonValueExpr structure specifying the expression whose values will become array elements
- `absent_on_null`: Boolean flag indicating whether to skip NULL values when constructing the array

## Dependencies
- Functions called/Symbols referenced:
  - JsonAggConstructor
  - JsonValueExpr
- Called from (representative examples):
  - exprLocation
  - transformExprRecurse
  - transformJsonArrayAgg
  - transformJsonArrayQueryConstructor
  - raw_expression_tree_walker_impl

## Notes and Other Information
- This structure is used during the parsing phase before transformation into execution-ready aggregate expressions
- The absent_on_null flag controls JSON array construction behavior when encountering NULL values
- Inherits all standard aggregate features (filtering, ordering, windowing) through the constructor field
- Unlike JsonObjectAgg, this structure doesn't need key uniqueness checking since arrays allow duplicate values
- Part of PostgreSQL's JSON aggregate function support introduced for SQL/JSON standard compliance
- The ordering specified in the constructor is particularly important for array aggregates as it determines element sequence