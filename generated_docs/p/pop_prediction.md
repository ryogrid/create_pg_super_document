# pop_prediction

## Location
[src/common/jsonapi.c:432-438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L432-L438)

## Overview
Removes and returns the top prediction token from the JSON parser's prediction stack.

## Definition
```c
static inline char pop_prediction(JsonParserStack *pstack)
```

## Detailed Description
The `pop_prediction` function implements a stack pop operation for the JSON parser's prediction mechanism. It decrements the prediction index and returns the character at that position in the prediction array. This function is used during predictive parsing to consume expected tokens from the prediction stack. The function includes an assertion to ensure the prediction stack is not empty before popping.

## Parameters / Member Variables
- `pstack`: Pointer to a `JsonParserStack` structure containing the parser's stack state and prediction buffer

## Dependencies
- Functions called/Symbols referenced:
  - [JsonParserStack](../J/JsonParserStack.md) (struct type)
  - Assert (macro for debugging checks)
- Called from (representative examples):
  - [pg_parse_json_incremental](pg_parse_json_incremental.md) (at src/common/jsonapi.c:687)

## Notes and Other Information
- This is a static inline function for performance optimization
- Returns a char representing the predicted token
- Includes an assertion to prevent underflow of the prediction stack
- The function pre-decrements the prediction index before accessing the array
- Essential for implementing the predictive parsing algorithm in the JSON grammar engine
- Part of the incremental JSON parsing infrastructure