# suppress_redundant_updates_trigger

## Location
[src/backend/utils/adt/trigfuncs.c:28-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/trigfuncs.c#L28-L84)

## Overview
A trigger function designed to optimize database performance by preventing redundant UPDATE operations when the OLD and NEW row data are identical.

## Definition

```c
Datum
suppress_redundant_updates_trigger(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL trigger function that performs a byte-level comparison between the old and new tuple data in an UPDATE operation. If the data is identical, it suppresses the update by returning NULL, which prevents unnecessary write operations, WAL logging, and potential cascading effects.

The function performs comprehensive validation to ensure it's called in the correct context:
- Must be invoked as a trigger (not a regular function)
- Must be fired by an UPDATE operation (not INSERT or DELETE)
- Must be a BEFORE trigger (not AFTER or INSTEAD OF)
- Must be a row-level trigger (not statement-level)

The comparison algorithm checks multiple aspects of the tuple structure:
- Overall tuple length ()
- Header offset () indicating where actual data begins
- Number of attributes using 
- Info mask flags (excluding transaction-related bits)
- Byte-by-byte comparison of the actual tuple data payload

If all these elements match exactly, the function returns NULL to suppress the update; otherwise, it returns the new tuple to allow the update to proceed.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention macro that provides access to:
  - : Contains  structure with trigger context information
  - : The proposed new tuple data
  - : The existing old tuple data
  - : Bitmask indicating trigger firing conditions

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to verify function is called as a trigger
  - : Macro to check if triggered by UPDATE
  - : Macro to verify it's a BEFORE trigger
  - : Macro to ensure row-level trigger
  - : Function to get number of attributes in tuple
  - : Error reporting function
  - : Converts pointer to PostgreSQL Datum
  - : Structure containing trigger execution context
  - : Structure representing tuple header information
  - : Bitmask for transaction-related flags
  - : Constant defining tuple header size

- Called from (representative examples):
  - No direct references found in the codebase (typically used via CREATE TRIGGER statements)

## Notes and Other Information
- This function is commonly used to optimize tables with frequent updates where many operations don't actually change data values
- The function is particularly useful for tables updated by ORMs or applications that issue UPDATE statements regardless of whether data has changed  
- Performance benefit comes from avoiding WAL logging, index updates, and trigger cascades for no-op updates
- The byte-level comparison is very thorough but may not catch semantically equivalent data stored in different binary formats
- Located in 
- Returns NULL to suppress update, or the new tuple pointer to allow update
- Must be created as a BEFORE UPDATE FOR EACH ROW trigger to function correctly
- Error messages use  for improper usage