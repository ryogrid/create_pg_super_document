# plperl_trigger_build_args

## Location
[src/pl/plperl/plperl.c:1631-1743](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1631-L1743)

## Overview
Constructs a comprehensive Perl hash reference containing all trigger-related information and arguments for PL/Perl trigger functions.

## Definition
static SV *plperl_trigger_build_args(FunctionCallInfo fcinfo)

## Detailed Description
This function creates a complete data structure that PL/Perl trigger functions receive as their argument. It extracts information from the PostgreSQL trigger context and organizes it into a Perl hash with standardized keys. The function handles all trigger types (INSERT, UPDATE, DELETE, TRUNCATE) and timing (BEFORE, AFTER, INSTEAD OF), providing access to both old and new tuple data where applicable. Special handling is implemented for generated columns in BEFORE triggers, where computed columns are not yet available in the NEW row. The function also converts trigger arguments, relation metadata, and timing information into Perl-accessible formats.

## Parameters / Member Variables
- `fcinfo`: PostgreSQL function call information structure containing the trigger context data

## Dependencies
- Functions called/Symbols referenced:
  - DirectFunctionCall1
  - [oidout](../o/oidout.md)
  - [DatumGetCString](../D/DatumGetCString.md)
  - [cstr2sv](../c/cstr2sv.md)
  - [hv_store_string](../h/hv_store_string.md)
  - [plperl_hash_from_tuple](plperl_hash_from_tuple.md)
  - newRV_noinc
  - [SPI_getrelname](../S/SPI_getrelname.md)
  - [SPI_getnspname](../S/SPI_getnspname.md)
  - TRIGGER_FIRED_BY_INSERT/UPDATE/DELETE/TRUNCATE
  - TRIGGER_FIRED_FOR_ROW/STATEMENT
  - TRIGGER_FIRED_BEFORE/AFTER/INSTEAD
- Called from (representative examples):
  - [plperl_trigger_handler](plperl_trigger_handler.md)

## Notes and Other Information
- Returns a hash reference with keys: name, relid, event, argc, args, relname, table_name, table_schema, when, level, old, new
- Handles generated columns correctly by excluding them from NEW row in BEFORE triggers
- Pre-grows hash to 12 elements for performance optimization
- Provides both "relname" and "table_name" keys for compatibility
- Converts trigger arguments array to Perl array reference when present
- Event types: INSERT, UPDATE, DELETE, TRUNCATE, or UNKNOWN
- Timing types: BEFORE, AFTER, INSTEAD OF, or UNKNOWN  
- Level types: ROW, STATEMENT, or UNKNOWN
- Old/new tuple data only included for row-level triggers where applicable