# push_prediction

## Location
[src/common/jsonapi.c:425-431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L425-L431)

## Overview
Adds a production rule to the JSON parser's prediction stack by copying the production sequence from a table entry.

## Definition
```c
static inline void push_prediction(JsonParserStack *pstack, td_entry entry)
```

## Detailed Description
The `push_prediction` function is used in JSON parsing to implement a predictive parsing algorithm. It takes a table entry containing a production rule and pushes it onto the parser's prediction stack. The function copies the production sequence from the entry's `prod` array to the current position in the prediction stack, then advances the prediction index by the length of the production. This mechanism allows the parser to predict and prepare for upcoming JSON tokens based on grammar rules.

## Parameters / Member Variables
- `pstack`: Pointer to a `JsonParserStack` structure containing the parser's stack state and prediction buffer
- `entry`: A `td_entry` structure containing the production rule data, including the production array (`prod`) and its length (`len`)

## Dependencies
- Functions called/Symbols referenced:
  - JsonParserStack (struct type)
  - td_entry (struct type)
- Called from (representative examples):
  - pg_parse_json_incremental (at src/common/jsonapi.c:682)
  - pg_parse_json_incremental (at src/common/jsonapi.c:714)

## Notes and Other Information
- This is a static inline function for performance optimization
- Part of the incremental JSON parsing implementation
- Uses memcpy for efficient copying of production sequences
- The function advances the prediction index to prepare for the next prediction operation
- Essential for implementing predictive parsing in the JSON grammar engine