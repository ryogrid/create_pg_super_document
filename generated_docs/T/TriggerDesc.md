# TriggerDesc

## Location
src/include/utils/reltrigger.h: 47 - 79

## Overview
TriggerDesc is a comprehensive descriptor structure that contains an array of triggers for a relation along with optimization flags that indicate the presence of specific trigger types to avoid unnecessary array scanning.

## Definition


## Detailed Description
TriggerDesc serves as an efficient container and index for all triggers associated with a database relation. The structure is designed with performance optimization in mind, using boolean flags to quickly determine whether specific types of triggers exist without needing to iterate through the entire triggers array. This design pattern is crucial for PostgreSQL's trigger execution system, where the database needs to quickly determine which triggers should fire for a given operation. The structure covers all possible combinations of trigger timing (BEFORE/AFTER/INSTEAD), events (INSERT/UPDATE/DELETE/TRUNCATE), and levels (row/statement), plus support for transition tables that provide OLD and NEW table references in statement-level triggers.

## Parameters / Member Variables
- : Pointer to array of Trigger structures containing the actual trigger definitions
- : Total number of triggers in the array
- : Flag indicating presence of BEFORE INSERT row-level triggers
- : Flag indicating presence of AFTER INSERT row-level triggers
- : Flag indicating presence of INSTEAD OF INSERT triggers (for views)
- : Flag indicating presence of BEFORE INSERT statement-level triggers
- : Flag indicating presence of AFTER INSERT statement-level triggers
- : Flag indicating presence of BEFORE UPDATE row-level triggers
- : Flag indicating presence of AFTER UPDATE row-level triggers
- : Flag indicating presence of INSTEAD OF UPDATE triggers (for views)
- : Flag indicating presence of BEFORE UPDATE statement-level triggers
- : Flag indicating presence of AFTER UPDATE statement-level triggers
- : Flag indicating presence of BEFORE DELETE row-level triggers
- : Flag indicating presence of AFTER DELETE row-level triggers
- : Flag indicating presence of INSTEAD OF DELETE triggers (for views)
- : Flag indicating presence of BEFORE DELETE statement-level triggers
- : Flag indicating presence of AFTER DELETE statement-level triggers
- : Flag indicating presence of BEFORE TRUNCATE statement-level triggers
- : Flag indicating presence of AFTER TRUNCATE statement-level triggers
- : Flag indicating presence of triggers using NEW transition table for INSERT
- : Flag indicating presence of triggers using OLD transition table for UPDATE
- : Flag indicating presence of triggers using NEW transition table for UPDATE
- : Flag indicating presence of triggers using OLD transition table for DELETE

## Dependencies
- Functions called/Symbols referenced:
  - Trigger (struct type for the triggers array)
- Called from (representative examples):
  - RelationBuildTriggers
  - SetTriggerFlags
  - ExecBSInsertTriggers
  - ExecASInsertTriggers
  - ExecBRInsertTriggers
  - has_row_triggers
  - MakeTransitionCaptureState

## Notes and Other Information
The optimization flags are crucial for performance in high-throughput scenarios where trigger checking occurs frequently. Note that TRUNCATE triggers only exist at the statement level (no row-level TRUNCATE triggers), which is reflected in the structure design. The transition table flags (trig_*_old_table, trig_*_new_table) are used to determine when OLD and NEW pseudo-relations need to be captured for statement-level triggers. This structure is typically built once when a relation is opened and cached for the lifetime of the relation, making the boolean flag optimization particularly effective for reducing repeated array scanning overhead.