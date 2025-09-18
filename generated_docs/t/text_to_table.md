# text_to_table

## Location
src/backend/utils/adt/varlena.c: 4551 - 4574

## Overview
Parses input string and returns a table (set-returning function) of elements based on a provided field separator.

## Definition
```c
Datum text_to_table(PG_FUNCTION_ARGS)
```

## Detailed Description
The text_to_table function is a PostgreSQL set-returning function (SRF) that splits a text string into individual rows of a table using a specified delimiter. Unlike text_to_array which returns an array, this function returns each split element as a separate row. It initializes a materialized SRF using InitMaterializedSRF and configures the output state to use a tuple store and tuple descriptor for row-based output. The actual splitting is delegated to the split_text function.

## Parameters / Member Variables
- Takes PostgreSQL function arguments via PG_FUNCTION_ARGS macro (typically text input string and delimiter)
- Uses ReturnSetInfo to manage set-returning function metadata
- Returns Datum 0 (the actual results are stored in the tuple store)

## Dependencies
- Functions called/Symbols referenced:
  - ReturnSetInfo (structure for SRF result information)
  - SplitTextOutputData (structure for output state)
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md) (initializes materialized set-returning function)
  - MAT_SRF_USE_EXPECTED_DESC (flag for using expected descriptor)
  - [split_text](../s/split_text.md) (core text splitting logic)
- Called from (representative examples):
  - [text_to_table_null](text_to_table_null.md)

## Notes and Other Information
- Located in src/backend/utils/adt/varlena.c:4551-4574
- This is a set-returning function that produces rows instead of arrays
- Uses PostgreSQL's materialized SRF infrastructure for efficient row output
- Sets astate to NULL since array output is not needed for table format
- Part of PostgreSQL's variable-length data type utilities