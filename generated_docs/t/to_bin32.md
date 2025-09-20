# to_bin32

## Location
[src/backend/utils/adt/varlena.c:4956-4962](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4956-L4962)

## Overview
Converts a 32-bit integer value to its binary (base-2) string representation.

## Definition

```c
Datum
to_bin32(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL SQL-callable function that takes a 32-bit signed integer and converts it to a text string containing the binary representation of that number. The function treats the input as an unsigned 32-bit value during conversion, ensuring consistent binary output regardless of the sign of the input.

The function serves as a wrapper around the internal  utility function, specifically configured for base-2 conversion. It's part of PostgreSQL's set of number-to-string conversion functions that allow users to represent numeric values in different bases through SQL.

## Parameters / Member Variables
- Function follows PostgreSQL's standard function calling convention using 
- Expects one argument: a 32-bit integer value accessed via 
- The input is cast to  to ensure unsigned interpretation before conversion to 

## Dependencies
- Functions called/Symbols referenced:
  -  (internal utility function for base conversion)
  -  (PostgreSQL macro for returning text values)
  -  (PostgreSQL macro for extracting int32 arguments)

- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch mechanism)

## Notes and Other Information
- Located in 
- The function explicitly casts the input to  before widening to , ensuring consistent binary representation
- Part of a family of base conversion functions including , , , and 
- The actual conversion logic is handled by the shared  static function
- Returns a PostgreSQL  type containing the binary string representation