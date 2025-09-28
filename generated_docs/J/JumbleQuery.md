# JumbleQuery

## Location
[src/backend/nodes/queryjumblefuncs.c:105-149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/queryjumblefuncs.c#L105-L149)

## Overview
JumbleQuery is the main function that generates a unique query identifier (queryId) by creating a normalized representation of a SQL query structure and computing its hash.

## Definition

```c
JumbleState *
JumbleQuery(Query *query)
```
## Detailed Description
JumbleQuery processes a parsed SQL query (Query node) to generate a unique identifier that can be used to group similar queries together regardless of parameter values or formatting differences. It creates a JumbleState workspace, recursively processes the query tree structure using _jumbleNode to create a normalized representation, and then computes a hash value that becomes the query's unique identifier. The function handles edge cases where the hash might be zero by using fallback values (1 for normal statements, 2 for utility statements).

## Parameters / Member Variables
- : Input Query node representing the parsed SQL statement for which to generate an identifier

## Dependencies
- Functions called/Symbols referenced:
  - [IsQueryIdEnabled](../I/IsQueryIdEnabled.md) (checks if query ID generation is enabled)
  - [_jumbleNode](../j/_jumbleNode.md) (recursively processes query tree nodes)
  - [hash_any_extended](../h/hash_any_extended.md) (computes hash from jumbled data)
  - [DatumGetUInt64](../D/DatumGetUInt64.md) (converts hash result to uint64)
  - [JumbleState](JumbleState.md), JUMBLE_SIZE, LocationLen (data structures and constants)
- Called from (representative examples):
  - [ExplainQuery](../E/ExplainQuery.md) (for EXPLAIN statement processing)
  - [parse_analyze_fixedparams](../p/parse_analyze_fixedparams.md), parse_analyze_varparams, parse_analyze_withcb (during query analysis)
  - COMPUTE_QUERY_ID_REGRESS (testing macro)

## Notes and Other Information
- Returns a JumbleState containing the jumbled representation and location information
- Sets the queryId field directly on the input Query node
- Allocates workspace memory for jumbling operations (JUMBLE_SIZE bytes)
- Maintains constant location tracking for parameter placeholders
- Uses zero-collision avoidance: assigns 1 for regular queries, 2 for utility statements when hash is zero
- Part of PostgreSQL's query normalization system used by pg_stat_statements and query planning
- Requires query ID generation to be enabled (checked via IsQueryIdEnabled)

## Simplified Source

```c
// Simplified version of JumbleQuery
JumbleState *JumbleQuery(Query *query) {
    JumbleState *jstate = NULL;

    Assert(IsQueryIdEnabled());

    // Allocate and initialize jumble state
    jstate = (JumbleState *) palloc(sizeof(JumbleState));

    // Set up workspace for query jumbling
    jstate->jumble = (unsigned char *) palloc(JUMBLE_SIZE);
    jstate->jumble_len = 0;
    jstate->clocations_buf_size = 32;
    jstate->clocations = (LocationLen *) palloc(jstate->clocations_buf_size * sizeof(LocationLen));
    jstate->clocations_count = 0;
    jstate->highest_extern_param_id = 0;

    // Generate normalized representation and compute hash
    _jumbleNode(jstate, (Node *) query);
    query->queryId = DatumGetUInt64(hash_any_extended(jstate->jumble,
                                                     jstate->jumble_len,
                                                     0));

    // Handle zero hash collision avoidance
    if (query->queryId == UINT64CONST(0)) {
        if (query->utilityStmt) {
            query->queryId = UINT64CONST(2);
        } else {
            query->queryId = UINT64CONST(1);
        }
    }

    return jstate;
}
```

Key simplifications made:
- Preserved complete query jumbling and hashing logic
- Maintained workspace initialization
- Kept zero collision avoidance handling
- Focused on core query ID generation functionality