# next_prediction

## Location
[src/common/jsonapi.c:439-445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L439-L445)

## Overview
Peeks at the top prediction token on the JSON parser's prediction stack without removing it.

## Definition
```c
static inline char next_prediction(JsonParserStack *pstack)
```

## Detailed Description
The `next_prediction` function provides a non-destructive look-ahead operation for the JSON parser's prediction mechanism. Unlike `pop_prediction`, this function returns the character at the top of the prediction stack without modifying the stack state. It accesses the prediction array at index `pred_index - 1` to peek at the next expected token. The function includes an assertion to ensure the prediction stack contains at least one element before attempting to peek.

## Parameters / Member Variables
- `pstack`: Pointer to a `JsonParserStack` structure containing the parser's stack state and prediction buffer

## Dependencies
- Functions called/Symbols referenced:
  - JsonParserStack (struct type)
  - Assert (macro for debugging checks)
- Called from (representative examples):
  - pg_parse_json_incremental (at src/common/jsonapi.c:938)
  - pg_parse_json_incremental (at src/common/jsonapi.c:965)

## Notes and Other Information
- This is a static inline function for performance optimization
- Returns a char representing the next predicted token without consuming it
- Non-destructive peek operation - does not modify the prediction stack
- Includes an assertion to prevent accessing an empty prediction stack
- Used for decision-making in the parsing algorithm where the parser needs to know what to expect next
- Complementary to `pop_prediction` which actually consumes the prediction
- Essential for implementing look-ahead functionality in the predictive JSON parser