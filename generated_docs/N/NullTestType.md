# NullTestType

## Location
[src/include/nodes/primnodes.h:1953-1954](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1953-L1954)

## Overview
NullTestType is an enumeration that defines the types of NULL testing operations available in PostgreSQL for checking whether values are NULL or NOT NULL.

## Definition
```c
typedef enum NullTestType
{
    IS_NULL, 
    IS_NOT_NULL
} NullTestType;
```

## Detailed Description
NullTestType specifies the type of NULL test to be performed on expressions, supporting the fundamental SQL operations IS NULL and IS NOT NULL. This enumeration is used within the NullTest structure to determine whether to test for the presence or absence of NULL values. The enum works in conjunction with the NullTest node to implement both simple NULL checks and more complex row-based NULL testing per SQL standard requirements. When combined with the argisrow flag in NullTest, it can handle field-by-field NULL checks for composite types.

## Parameters / Member Variables
- `IS_NULL`: Tests whether the expression evaluates to NULL (implements "IS NULL" operation)
- `IS_NOT_NULL`: Tests whether the expression does not evaluate to NULL (implements "IS NOT NULL" operation)

## Dependencies
- Functions called/Symbols referenced:
  - None (this is an enumeration)
- Called from (representative examples):
  - [nulltestsel](../n/nulltestsel.md) (selectivity estimation for NULL tests)
  - [NullTest](NullTest.md) struct (used as nulltesttype field)
  - [GenericCosts](../G/GenericCosts.md) (cost estimation structure)

## Notes and Other Information
- Used in conjunction with NullTest structure for implementing NULL testing operations
- Supports both simple value NULL tests and complex row-type NULL testing
- When used with row types and argisrow=true, implements "row IS [NOT] NULL" per SQL standard
- When used with row types and argisrow=false, implements "row IS [NOT] DISTINCT FROM NULL"
- The NULL test operation returns a boolean Datum indicating the result
- Critical for SQL compliance in handling NULL value semantics
- Used by the query planner for selectivity estimation and cost calculations
- Fundamental to PostgreSQL's implementation of three-valued logic (TRUE/FALSE/NULL)