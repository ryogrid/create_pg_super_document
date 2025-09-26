# PLy_trigger_build_args

## Location
[src/pl/plpython/plpy_exec.c:705-921](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_exec.c#L705-L921)

## Overview
Builds a Python dictionary containing trigger-related information and arguments for PL/Python trigger functions, providing access to trigger context, table metadata, old/new tuples, and trigger arguments.

## Definition

```c
static PyObject *
PLy_trigger_build_args(FunctionCallInfo fcinfo, PLyProcedure *proc, HeapTuple *rv)
```
## Detailed Description
This function constructs a comprehensive Python dictionary that contains all the information needed by a PL/Python trigger function. It extracts trigger metadata from the TriggerData structure and converts PostgreSQL data types to their Python equivalents. The function handles different trigger types (BEFORE/AFTER/INSTEAD OF), trigger levels (ROW/STATEMENT), and trigger events (INSERT/DELETE/UPDATE/TRUNCATE). For row-level triggers, it converts the old and new tuples to Python objects, while for statement-level triggers, it sets these to None. The function also handles trigger arguments and provides table metadata such as relation ID, table name, and schema name.

## Parameters / Member Variables
- : Function call information containing the trigger context data
- : PL/Python procedure information including input conversion functions  
- : Output parameter that receives the HeapTuple to be returned by the trigger

## Dependencies
- Functions called/Symbols referenced:
  - [PLyUnicode_FromString](PLyUnicode_FromString.md)
  - [PLy_input_from_tuple](PLy_input_from_tuple.md)
  - DirectFunctionCall1
  - [DatumGetCString](../D/DatumGetCString.md)
  - [SPI_getrelname](../S/SPI_getrelname.md)
  - [SPI_getnspname](../S/SPI_getnspname.md)
  - TRIGGER_FIRED_BEFORE/AFTER/INSTEAD
  - TRIGGER_FIRED_FOR_ROW/STATEMENT
  - TRIGGER_FIRED_BY_INSERT/DELETE/UPDATE/TRUNCATE
- Called from (representative examples):
  - [PLy_exec_trigger](PLy_exec_trigger.md)

## Notes and Other Information
The function creates a Python dictionary with the following keys:
- 'name': Trigger name
- 'relid': Relation OID as string
- 'table_name': Table name
- 'table_schema': Schema name  
- 'when': Trigger timing ('BEFORE', 'AFTER', 'INSTEAD OF')
- 'level': Trigger level ('ROW', 'STATEMENT')
- 'event': Trigger event ('INSERT', 'DELETE', 'UPDATE', 'TRUNCATE')
- 'old': Old tuple for row-level triggers (None for statement-level)
- 'new': New tuple for row-level triggers (None for statement-level)
- 'args': List of trigger arguments

The function uses PG_TRY/PG_CATCH blocks for proper error handling and Python reference counting. For BEFORE triggers on row-level operations, stored generated columns are not included in the NEW tuple as they haven't been computed yet.