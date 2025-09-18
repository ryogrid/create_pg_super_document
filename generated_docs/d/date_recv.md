# date_recv

## Location
[src/backend/utils/adt/date.c:209-230](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L209-L230)

## Overview
Converts PostgreSQL date values from external binary format to the internal DateADT representation used by PostgreSQL.

## Definition
```c
Datum date_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `date_recv` function is responsible for converting date values from PostgreSQL's external binary protocol format into the internal DateADT (Date Abstract Data Type) representation. This function is part of PostgreSQL's type input/output system and is used when date values are received from clients using the binary protocol format rather than text format. The function includes validation to ensure the received date value falls within PostgreSQL's supported date range, rejecting values outside the valid range with an appropriate error message.

## Parameters / Member Variables
- `buf`: StringInfo pointer containing the binary data received from the client protocol stream

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint](../p/pq_getmsgint.md): Extracts integer values from the message buffer
  - DATE_NOT_FINITE: Macro to check for special infinite date values
  - IS_VALID_DATE: Macro to validate date values are within acceptable range
  - ereport: PostgreSQL error reporting function
  - PG_RETURN_DATEADT: Macro to return DateADT values from PostgreSQL functions
- Called from (representative examples):
  - No direct references found (likely referenced through function pointers in type system)

## Notes and Other Information
- This function is part of PostgreSQL's binary I/O system for the date data type
- Special infinite date values (positive and negative infinity) are accepted without range validation
- Range validation uses the same limits as the text input function date_in()
- Returns ERRCODE_DATETIME_VALUE_OUT_OF_RANGE error for invalid dates
- The function follows PostgreSQL's standard function calling conventions using PG_FUNCTION_ARGS