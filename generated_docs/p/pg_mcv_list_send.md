# pg_mcv_list_send

## Location
[src/backend/statistics/mcv.c:1523-1534](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mcv.c#L1523-L1534)

## Overview
Binary output routine for the pg_mcv_list data type that converts MCV (Most Common Values) list data to its binary representation for network transmission.

## Definition
```c
Datum pg_mcv_list_send(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the binary output routine for the pg_mcv_list data type in PostgreSQL's type system. MCV lists are serialized in a bytea value (although the type is named differently), so this function simply delegates to the existing byteasend function to handle the binary serialization. This is part of PostgreSQL's infrastructure for converting internal data types to their binary wire format for network transmission or storage.

## Parameters / Member Variables
- Uses the standard PostgreSQL function call convention through `PG_FUNCTION_ARGS`, which provides access to the function call information including arguments

## Dependencies
- Functions called/Symbols referenced:
  - [byteasend](../b/byteasend.md)
- Called from (representative examples):
  - (No direct callers found - typically called by PostgreSQL's type system infrastructure)

## Notes and Other Information
- This function is part of PostgreSQL's type input/output framework
- MCV lists are stored internally as bytea values, making the delegation to byteasend appropriate
- The function follows the standard PostgreSQL convention for type send functions
- Located in src/backend/statistics/mcv.c:1523-1534