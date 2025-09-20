# makeNumericAggStateCurrentContext

## Location
[src/backend/utils/adt/numeric.c:4858-4872](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4858-L4872)

## Overview
Creates and initializes a NumericAggState structure in the current memory context, used for situations where aggregate context validation is not needed.

## Definition

```c
static NumericAggState *
makeNumericAggStateCurrentContext(bool calcSumX2)
```
## Detailed Description
This static function is a simpler variant of  that creates a NumericAggState structure without performing aggregate context validation or memory context switching. It directly allocates the state structure in the current memory context using  for zero-initialization. This function is typically used in scenarios where the caller has already ensured the appropriate memory context or where the state needs to be allocated in a specific context (such as during deserialization or combine operations). The function sets the sum-of-squares calculation flag and stores a reference to the current memory context.

## Parameters / Member Variables
- : Boolean flag indicating whether the aggregate should calculate sum of squares in addition to sum and count

## Dependencies
- Functions called/Symbols referenced:
  -  - Allocate zero-initialized memory
  -  - Structure type for aggregate state
  -  - Global variable referencing the current memory context
- Called from (representative examples):
  -  - Combine numeric aggregate states
  -  - Combine numeric average aggregate states
  -  - Deserialize numeric average aggregate state
  -  - Deserialize numeric aggregate state
  -  - Polymorphic version for current context

## Notes and Other Information
- Declared as static, limiting visibility to numeric.c file
- Does not perform aggregate context validation unlike 
- Directly uses  without switching contexts
- Commonly used in deserialization and state combination operations
- Uses  to ensure all fields are properly initialized to zero
- More lightweight than  due to lack of context validation
- Located in src/backend/utils/adt/numeric.c:4858-4872