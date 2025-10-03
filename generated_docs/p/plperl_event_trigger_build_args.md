# plperl_event_trigger_build_args

## Location
[src/pl/plperl/plperl.c:1744-1761](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1744-L1761)

## Overview
Builds Perl hash arguments for event trigger functions by extracting event information from the PostgreSQL function call context.

## Definition

```c
static SV  *
plperl_event_trigger_build_args(FunctionCallInfo fcinfo)
```
## Detailed Description
This function prepares the argument structure passed to Perl event trigger functions. It extracts event trigger data from the PostgreSQL function call context and packages it into a Perl hash reference containing the event name and command tag. The function serves as a bridge between PostgreSQL's internal event trigger representation and the Perl interface, ensuring that Perl event trigger functions receive properly formatted event information.

## Parameters / Member Variables
- `fcinfo`: PostgreSQL function call information structure containing the event trigger context data
## Dependencies
- Functions called/Symbols referenced:
  - newHV (Perl API function to create new hash)
  - [EventTriggerData](../E/EventTriggerData.md) (PostgreSQL event trigger data structure)
  - [hv_store_string](../h/hv_store_string.md) (utility function to store string values in Perl hash)
  - [cstr2sv](../c/cstr2sv.md) (conversion function from C string to Perl scalar)
  - [GetCommandTagName](../G/GetCommandTagName.md) (PostgreSQL function to get command tag name)
  - newRV_noinc (Perl API function to create reference without incrementing reference count)
- Called from (representative examples):
  - [plperl_event_trigger_handler](plperl_event_trigger_handler.md)

## Notes and Other Information
- Returns a Perl hash reference with 'event' and 'tag' keys
- Uses dTHX macro for Perl thread context (required for multi-threaded Perl operations)
- The returned hash contains the event name (e.g., 'ddl_command_start') and the SQL command tag (e.g., 'CREATE TABLE')
- Part of the PL/Perl language extension's event trigger support system