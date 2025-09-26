# SetConstraintTriggerData

## Location
[src/backend/commands/trigger.c:3632-3636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L3632-L3636)

## Overview
SetConstraintTriggerData is a structure that stores the constraint status information for individual triggers in PostgreSQL's SET CONSTRAINTS implementation.

## Definition

```c
typedef struct SetConstraintTriggerData
{
	Oid			sct_tgoid;
	bool		sct_tgisdeferred;
} SetConstraintTriggerData;
```
## Detailed Description
This structure is part of PostgreSQL's constraint deferral mechanism. It maintains the current deferral state for a specific trigger object. When a SET CONSTRAINTS command is executed, the system needs to track which constraint triggers should be deferred (delayed until transaction commit) versus immediate execution. Each SetConstraintTriggerData entry corresponds to one constraint trigger and stores its current deferral setting.

## Parameters / Member Variables
- `sct_tgoid`: Object ID (Oid) of the trigger whose constraint status is being tracked
- `sct_tgisdeferred`: Boolean flag indicating whether this trigger is currently set to deferred mode (true) or immediate mode (false)
## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [SetConstraintTrigger](SetConstraintTrigger.md)
  - [SetConstraintStateData](SetConstraintStateData.md)
  - [SetConstraintStateCreate](SetConstraintStateCreate.md)
  - [SetConstraintStateCopy](SetConstraintStateCopy.md)
  - [SetConstraintStateAddItem](SetConstraintStateAddItem.md)

## Notes and Other Information
- This structure is used within the after-trigger system for managing constraint deferral
- Part of PostgreSQL's transaction-level constraint management where DEFERRABLE constraints can be set to check either immediately or at transaction commit time
- The structure is lightweight by design to minimize memory consumption when tracking many constraint triggers
- Located in src/backend/commands/trigger.c as part of the trigger execution subsystem