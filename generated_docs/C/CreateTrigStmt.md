# CreateTrigStmt

## Location
[src/include/nodes/parsenodes.h:3001-3023](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3001-L3023)

## Overview
CreateTrigStmt represents the parsed structure of a CREATE TRIGGER SQL statement, used to define database triggers that execute automatically in response to table events.

## Definition
```c
typedef struct CreateTrigStmt
{
	NodeTag		type;
	bool		replace;		/* replace trigger if already exists */
	bool		isconstraint;	/* This is a constraint trigger */
	char	   *trigname;		/* TRIGGER's name */
	RangeVar   *relation;		/* relation trigger is on */
	List	   *funcname;		/* qual. name of function to call */
	List	   *args;			/* list of String or NIL */
	bool		row;			/* ROW/STATEMENT */
	int16		timing;			/* BEFORE, AFTER, or INSTEAD */
	int16		events;			/* "OR" of INSERT/UPDATE/DELETE/TRUNCATE */
	List	   *columns;		/* column names, or NIL for all columns */
	Node	   *whenClause;		/* qual expression, or NULL if none */
	List	   *transitionRels; /* TriggerTransition nodes, or NIL if none */
	bool		deferrable;		/* [NOT] DEFERRABLE */
	bool		initdeferred;	/* INITIALLY {DEFERRED|IMMEDIATE} */
	RangeVar   *constrrel;		/* opposite relation, if RI trigger */
} CreateTrigStmt;
```

## Detailed Description
CreateTrigStmt is a parse tree node that captures all components of a CREATE TRIGGER statement. Triggers are special stored procedures that run automatically when certain database events occur. This structure supports both regular triggers (for business logic) and constraint triggers (for referential integrity). The timing field uses TRIGGER_TYPE_* constants from catalog/pg_trigger.h to specify when the trigger fires (BEFORE, AFTER, or INSTEAD OF), while the events field specifies which operations (INSERT, UPDATE, DELETE, TRUNCATE) activate the trigger.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a CreateTrigStmt node
- `replace`: Whether to replace an existing trigger with the same name (CREATE OR REPLACE)
- `isconstraint`: True if this is a constraint trigger (for referential integrity)
- `trigname`: Name of the trigger being created
- `relation`: RangeVar representing the table the trigger is attached to
- `funcname`: Qualified name of the trigger function to execute
- `args`: List of string arguments to pass to the trigger function (or NIL)
- `row`: True for row-level triggers, false for statement-level triggers
- `timing`: When the trigger fires (TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_AFTER, or TRIGGER_TYPE_INSTEAD)
- `events`: Bitmap of triggering events (TRIGGER_TYPE_INSERT, TRIGGER_TYPE_UPDATE, etc.)
- `columns`: List of column names for UPDATE triggers (or NIL for all columns)
- `whenClause`: Optional WHEN condition expression for conditional firing
- `transitionRels`: List of TriggerTransition nodes for OLD/NEW table references
- `deferrable`: Whether the constraint trigger can be deferred (constraint triggers only)
- `initdeferred`: Initial deferral state (constraint triggers only)
- `constrrel`: Referenced table for referential integrity triggers (constraint triggers only)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for creating the node)
  - RangeVar (for table references)
  - TRIGGER_TYPE_* constants (for timing and events)
  - TriggerTransition (for transition table specifications)
- Called from (representative examples):
  - CreateTrigger (in src/backend/commands/trigger.c:159)
  - CreateTriggerFiringOn (in src/backend/commands/trigger.c:176)
  - ProcessUtilitySlow (utility command processing)

## Notes and Other Information
- Part of PostgreSQL's trigger system for automated response to data changes
- Supports both regular triggers and constraint triggers for referential integrity
- The timing and events fields use bitmasks defined in catalog/pg_trigger.h
- Row-level triggers execute once per affected row, statement-level triggers execute once per statement
- INSTEAD OF triggers are only supported on views
- Constraint triggers support deferral mechanisms for transaction-end constraint checking
- Processed by CreateTrigger and related functions in src/backend/commands/trigger.c