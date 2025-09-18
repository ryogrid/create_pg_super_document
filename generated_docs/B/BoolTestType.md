# BoolTestType

## Location
[src/include/nodes/primnodes.h:1977-1978](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1977-L1978)

## Overview
BoolTestType is an enumeration that defines the types of boolean testing operations for determining whether a boolean value is TRUE, FALSE, or UNKNOWN (NULL) in PostgreSQL's three-valued logic system.

## Definition
```c
typedef enum BoolTestType
{
    IS_TRUE, 
    IS_NOT_TRUE, 
    IS_FALSE, 
    IS_NOT_FALSE, 
    IS_UNKNOWN, 
    IS_NOT_UNKNOWN
} BoolTestType;
```

## Detailed Description
BoolTestType supports all six meaningful combinations of boolean testing operations in PostgreSQL's implementation of three-valued logic. Unlike standard boolean operations, these tests handle NULL values explicitly and always return a definitive boolean result rather than propagating NULL. The enumeration is used within the BooleanTest structure to specify which boolean test should be performed on an expression. This is essential for SQL compliance where boolean expressions can have three states: TRUE, FALSE, or UNKNOWN (NULL).

## Parameters / Member Variables
- `IS_TRUE`: Tests whether the boolean expression evaluates to TRUE (implements "IS TRUE" operation)
- `IS_NOT_TRUE`: Tests whether the boolean expression does not evaluate to TRUE (implements "IS NOT TRUE" operation, returns TRUE for FALSE or NULL)
- `IS_FALSE`: Tests whether the boolean expression evaluates to FALSE (implements "IS FALSE" operation)
- `IS_NOT_FALSE`: Tests whether the boolean expression does not evaluate to FALSE (implements "IS NOT FALSE" operation, returns TRUE for TRUE or NULL)
- `IS_UNKNOWN`: Tests whether the boolean expression evaluates to UNKNOWN/NULL (implements "IS UNKNOWN" operation)
- `IS_NOT_UNKNOWN`: Tests whether the boolean expression does not evaluate to UNKNOWN/NULL (implements "IS NOT UNKNOWN" operation, returns TRUE for TRUE or FALSE)

## Dependencies
- Functions called/Symbols referenced:
  - None (this is an enumeration)
- Called from (representative examples):
  - booltestsel (selectivity estimation for boolean tests)
  - BooleanTest struct (used as booltesttype field)
  - GenericCosts (cost estimation structure)

## Notes and Other Information
- Implements complete three-valued boolean logic testing as required by SQL standard
- NULL input values do not cause NULL results; tests always return definitive boolean values
- Essential for handling SQL's three-valued logic where expressions can be TRUE, FALSE, or UNKNOWN
- Used by the query planner for selectivity estimation and cost calculations
- IS_NOT_TRUE returns TRUE for both FALSE and NULL inputs
- IS_NOT_FALSE returns TRUE for both TRUE and NULL inputs  
- IS_UNKNOWN and IS_NOT_UNKNOWN specifically test for NULL boolean values
- Critical for proper SQL boolean expression evaluation and WHERE clause processing
- Works in conjunction with BooleanTest structure for complete boolean testing functionality