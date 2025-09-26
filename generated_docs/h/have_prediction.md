# have_prediction

## Location
[src/common/jsonapi.c:446-451](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/jsonapi.c#L446-L451)

## Overview
Checks whether the JSON parser's prediction stack contains any prediction tokens.

## Definition
```c
static inline bool have_prediction(JsonParserStack *pstack)
```

## Detailed Description
The `have_prediction` function is a simple predicate that determines whether the JSON parser's prediction stack is empty or contains prediction tokens. It returns true if the prediction index is greater than 0, indicating that there are predictions available on the stack, and false if the stack is empty. This function is essential for controlling the flow of the predictive parsing algorithm, allowing the parser to decide whether to use predictions or fall back to other parsing strategies.

## Parameters / Member Variables
- `pstack`: Pointer to a `JsonParserStack` structure containing the parser's stack state and prediction buffer

## Dependencies
- Functions called/Symbols referenced:
  - JsonParserStack (struct type)
- Called from (representative examples):
  - pg_parse_json_incremental (at src/common/jsonapi.c:678)
  - pg_parse_json_incremental (at src/common/jsonapi.c:685)

## Notes and Other Information
- This is a static inline function for performance optimization
- Returns a boolean value indicating the presence of predictions
- Used as a guard condition before calling other prediction-related functions
- Essential for implementing conditional logic in the predictive parsing algorithm
- Helps prevent accessing empty prediction stacks
- Simple but crucial for the control flow of the incremental JSON parser