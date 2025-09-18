# EventTriggerCollectAlterOpFam

## Location
src/backend/commands/event_trigger.c: 1795 - 1827

## Overview
Saves data about an ALTER OPERATOR FAMILY ADD/DROP command being executed for event trigger processing, collecting information about operator family modifications.

## Definition


## Detailed Description
This function is part of PostgreSQL's event trigger system and is responsible for collecting information about ALTER OPERATOR FAMILY commands during their execution. It captures details about modifications made to operator families, including both ADD and DROP operations.

The function creates a CollectedCommand structure with type SCT_AlterOpFamily and stores the operator family OID, lists of affected operators and procedures, and a copy of the original statement. This information enables event triggers to access comprehensive details about operator family changes.

The collected data includes the specific operator family being modified (identified by its OID), the lists of operators and procedures being added or dropped, and a deep copy of the parse tree representing the original ALTER OPERATOR FAMILY statement.

## Parameters / Member Variables
- : Pointer to an AlterOpFamilyStmt structure representing the parsed ALTER OPERATOR FAMILY command
- : OID of the operator family being modified
- : List of operators being added to or dropped from the operator family
- : List of support procedures being added to or dropped from the operator family

## Dependencies
- Functions called/Symbols referenced:
  -  - Memory context switching
  -  - Memory allocation in current context
  -  - Object address initialization macro
  -  - Deep copy of parse tree nodes
  -  - List append operation
  -  - Command type constant
  -  - Relation OID constant
- Called from (representative examples):
  -  - Adding operators/procedures to operator family
  -  - Dropping operators/procedures from operator family

## Notes and Other Information
- Only operates when event trigger context is active and command collection is not inhibited
- Uses the event trigger's memory context to ensure collected data persists beyond the current operation
- Sets the command's in_extension field based on the creating_extension global variable
- Creates a proper ObjectAddress for the operator family using ObjectAddressSet macro
- Stores both the operators and procedures lists directly (not deep copied) since they are assumed to have appropriate lifetime
- Part of PostgreSQL's operator class and operator family management system
- Enables event triggers to monitor changes to the operator infrastructure used by indexes and other database operations