# pg_event_trigger_dropped_objects

## Location
src/backend/commands/event_trigger.c: 1397 - 1492

## Overview
A PostgreSQL built-in function that returns information about objects dropped during the current DDL command, available only within sql_drop event trigger functions.

## Definition


## Detailed Description
This function provides access to the list of dropped objects that have been registered during the execution of the current DDL command. It can only be called from within sql_drop event trigger functions and returns a set of rows containing detailed information about each dropped object.

The function implements PostgreSQL's set-returning function (SRF) protocol and builds a tuplestore containing information about each object in the current event trigger state's SQLDropList. For each dropped object, it returns a comprehensive set of attributes including object identifiers, metadata, and categorization information.

The function enforces strict calling context validation - it will raise an error if called outside of a sql_drop event trigger function, ensuring it's used only in the appropriate event trigger context where dropped object information is meaningful and available.

## Parameters / Member Variables
- No direct parameters (uses PG_FUNCTION_ARGS macro)
- Returns a set of tuples with the following columns:
  - : OID of the object's catalog table
  - : OID of the dropped object
  - : Sub-object ID (for composite objects like table columns)  
  - : Boolean indicating if this was an original drop (not cascaded)
  - : Boolean indicating if this was a normal drop operation
  - : Boolean indicating if the object was temporary
  - : Text description of the object type
  - : Name of the schema containing the object (nullable)
  - : Name of the object (nullable)
  - : Full identity string of the object (nullable)
  - : Array of name components identifying the object
  - : Array of argument components for functions/operators

## Dependencies
- Functions called/Symbols referenced:
  - InitMaterializedSRF (initializes set-returning function infrastructure)
  - slist_foreach, slist_container (iterates through SQLDropList)
  - ObjectIdGetDatum, Int32GetDatum, BoolGetDatum, CStringGetTextDatum (datum conversion)
  - strlist_to_textarray (converts string lists to PostgreSQL arrays)
  - construct_empty_array (creates empty arrays for null cases)
  - tuplestore_putvalues (adds rows to result tuplestore)
- Called from:
  - No direct references (invoked by SQL as built-in function)

## Notes and Other Information
- Accessible only within sql_drop event trigger functions - raises ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED if called elsewhere
- Returns comprehensive object information collected by EventTriggerSQLDropAddObject
- Uses PostgreSQL's materialized set-returning function protocol
- Returns 12 columns of information per dropped object
- Handles nullable fields appropriately (schema_name, object_name, object_identity, address components)
- Essential for event trigger functions that need to audit or replicate DDL drop operations
- Located in src/backend/commands/event_trigger.c:1397-1492