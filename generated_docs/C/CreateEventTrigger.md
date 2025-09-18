# CreateEventTrigger

## Location
src/backend/commands/event_trigger.c: 120 - 211

## Overview
Creates a new event trigger in the PostgreSQL database, handling validation, permission checks, and catalog insertion for triggers that fire on specific database events.

## Definition


## Detailed Description
CreateEventTrigger is the main function responsible for creating event triggers in PostgreSQL. Event triggers are special triggers that fire on DDL events (like CREATE, ALTER, DROP commands), login events, or table rewrite operations across the entire database rather than on specific tables. The function performs comprehensive validation including superuser privilege checks, event name validation, filter condition parsing, tag validation, function signature verification, and prevents duplicate trigger names before inserting the new trigger into the system catalogs.

## Parameters / Member Variables
- : A CreateEventTrigStmt structure containing all the information needed to create the event trigger, including trigger name, event name, function name, and filter conditions (WHEN clauses)

## Dependencies
- Functions called/Symbols referenced:
  - superuser() - checks if current user has superuser privileges
  - error_duplicate_filter_variable() - reports error for duplicate filter variables
  - validate_ddl_tags() - validates tag filters for DDL events
  - validate_table_rewrite_tags() - validates tag filters for table rewrite events
  - SearchSysCache1() - searches system catalog for existing triggers
  - LookupFuncName() - looks up the trigger function
  - get_func_rettype() - gets the return type of the function
  - insert_event_trigger_tuple() - inserts the new trigger into catalogs
  - CStringGetDatum() - converts C string to Datum
  - NameListToString() - converts function name list to string
- Called from (representative examples):
  - standard_ProcessUtility() - main utility command processing function

## Notes and Other Information
- Requires superuser privileges to create event triggers due to privilege escalation risks
- Supports five event types: ddl_command_start, ddl_command_end, sql_drop, login, and table_rewrite
- Tag filtering is supported for DDL and table rewrite events but not for login events
- The trigger function must return type 'event_trigger'
- Prevents creation of duplicate event triggers with the same name
- Returns the OID of the newly created event trigger
- Part of PostgreSQL's event trigger system introduced to provide hooks for DDL auditing and replication tools