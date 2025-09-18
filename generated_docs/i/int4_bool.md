# int4_bool

## Location
[src/backend/utils/adt/int.c:362-371](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L362-L371)

## Overview
Converts a 32-bit integer (int4) value to a boolean value, following PostgreSQL's type casting semantics.

## Definition


## Detailed Description
This function implements the type conversion from PostgreSQL's int4 (32-bit integer) data type to boolean data type. The conversion follows standard C semantics where 0 evaluates to false and any non-zero value evaluates to true. This function is used internally by PostgreSQL's type system when explicit or implicit casting from integer to boolean is required.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments through the function call context
  - Argument 0: int4 value to be converted (accessed via )

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract int32 argument from function arguments
  - : Macro to return boolean value from PostgreSQL function
- Called from (representative examples):
  - PostgreSQL type casting system (no direct references found in current analysis)

## Notes and Other Information
- Located in 
- This is a standard PostgreSQL V1 calling convention function
- The conversion is straightforward: 0 → false, any non-zero value → true
- Used for explicit casts like  or implicit casts in boolean contexts