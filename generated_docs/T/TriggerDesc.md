# TriggerDesc

## Location
[src/include/utils/reltrigger.h:47-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/reltrigger.h#L47-L79)

## Overview
TriggerDesc is a comprehensive descriptor structure that contains an array of triggers for a relation along with optimization flags that indicate the presence of specific trigger types to avoid unnecessary array scanning.

## Definition

```c
typedef struct TriggerDesc
{
	Trigger    *triggers;		/* array of Trigger structs */
	int			numtriggers;	/* number of array entries */

	/*
	 * These flags indicate whether the array contains at least one of each
	 * type of trigger.  We use these to skip searching the array if not.
	 */
	bool		trig_insert_before_row;
	bool		trig_insert_after_row;
	bool		trig_insert_instead_row;
	bool		trig_insert_before_statement;
	bool		trig_insert_after_statement;
	bool		trig_update_before_row;
	bool		trig_update_after_row;
	bool		trig_update_instead_row;
	bool		trig_update_before_statement;
	bool		trig_update_after_statement;
	bool		trig_delete_before_row;
	bool		trig_delete_after_row;
	bool		trig_delete_instead_row;
	bool		trig_delete_before_statement;
	bool		trig_delete_after_statement;
	/* there are no row-level truncate triggers */
	bool		trig_truncate_before_statement;
	bool		trig_truncate_after_statement;
	/* Is there at least one trigger specifying each transition relation? */
	bool		trig_insert_new_table;
	bool		trig_update_old_table;
	bool		trig_update_new_table;
	bool		trig_delete_old_table;
} TriggerDesc;
```
## Detailed Description
TriggerDesc serves as an efficient container and index for all triggers associated with a database relation. The structure is designed with performance optimization in mind, using boolean flags to quickly determine whether specific types of triggers exist without needing to iterate through the entire triggers array. This design pattern is crucial for PostgreSQL's trigger execution system, where the database needs to quickly determine which triggers should fire for a given operation. The structure covers all possible combinations of trigger timing (BEFORE/AFTER/INSTEAD), events (INSERT/UPDATE/DELETE/TRUNCATE), and levels (row/statement), plus support for transition tables that provide OLD and NEW table references in statement-level triggers.

## Parameters / Member Variables
- `*triggers`: Pointer to array of Trigger structures containing the actual trigger definitions
- `numtriggers`: Total number of triggers in the array
- `trig_insert_before_row`: Flag indicating presence of BEFORE INSERT row-level triggers
- `trig_insert_after_row`: Flag indicating presence of AFTER INSERT row-level triggers
- `trig_insert_instead_row`: Flag indicating presence of INSTEAD OF INSERT triggers (for views)
- `trig_insert_before_statement`: Flag indicating presence of BEFORE INSERT statement-level triggers
- `trig_insert_after_statement`: Flag indicating presence of AFTER INSERT statement-level triggers
- `trig_update_before_row`: Flag indicating presence of BEFORE UPDATE row-level triggers
- `trig_update_after_row`: Flag indicating presence of AFTER UPDATE row-level triggers
- `trig_update_instead_row`: Flag indicating presence of INSTEAD OF UPDATE triggers (for views)
- `trig_update_before_statement`: Flag indicating presence of BEFORE UPDATE statement-level triggers
- `trig_update_after_statement`: Flag indicating presence of AFTER UPDATE statement-level triggers
- `trig_delete_before_row`: Flag indicating presence of BEFORE DELETE row-level triggers
- `trig_delete_after_row`: Flag indicating presence of AFTER DELETE row-level triggers
- `trig_delete_instead_row`: Flag indicating presence of INSTEAD OF DELETE triggers (for views)
- `trig_delete_before_statement`: Flag indicating presence of BEFORE DELETE statement-level triggers
- `trig_delete_after_statement`: Flag indicating presence of AFTER DELETE statement-level triggers
- `trig_truncate_before_statement`: Flag indicating presence of BEFORE TRUNCATE statement-level triggers
- `trig_truncate_after_statement`: Flag indicating presence of AFTER TRUNCATE statement-level triggers
- `trig_insert_new_table`: Flag indicating presence of triggers using NEW transition table for INSERT
- `trig_update_old_table`: Flag indicating presence of triggers using OLD transition table for UPDATE
- `trig_update_new_table`: Flag indicating presence of triggers using NEW transition table for UPDATE
- `trig_delete_old_table`: Flag indicating presence of triggers using OLD transition table for DELETE
## Dependencies
- Functions called/Symbols referenced:
  - [Trigger](Trigger.md) (struct type for the triggers array)
- Called from (representative examples):
  - [RelationBuildTriggers](../R/RelationBuildTriggers.md)
  - [SetTriggerFlags](../S/SetTriggerFlags.md)
  - [ExecBSInsertTriggers](../E/ExecBSInsertTriggers.md)
  - [ExecASInsertTriggers](../E/ExecASInsertTriggers.md)
  - [ExecBRInsertTriggers](../E/ExecBRInsertTriggers.md)
  - [has_row_triggers](../h/has_row_triggers.md)
  - [MakeTransitionCaptureState](../M/MakeTransitionCaptureState.md)

## Notes and Other Information
The optimization flags are crucial for performance in high-throughput scenarios where trigger checking occurs frequently. Note that TRUNCATE triggers only exist at the statement level (no row-level TRUNCATE triggers), which is reflected in the structure design. The transition table flags (trig_*_old_table, trig_*_new_table) are used to determine when OLD and NEW pseudo-relations need to be captured for statement-level triggers. This structure is typically built once when a relation is opened and cached for the lifetime of the relation, making the boolean flag optimization particularly effective for reducing repeated array scanning overhead.