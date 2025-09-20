# _ruleInfo

## Location
[src/bin/pg_dump/pg_dump.h:442-450](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L442-L450)

## Overview
The  structure represents PostgreSQL rules that need to be dumped and restored by pg_dump, including view rules and custom table rules.

## Definition

```c
typedef struct _ruleInfo
{
	DumpableObject dobj;
	TableInfo  *ruletable;		/* link to table the rule is for */
	char		ev_type;
	bool		is_instead;
	char		ev_enabled;
	bool		separate;		/* true if must dump as separate item */
	/* separate is always true for non-ON SELECT rules */
} RuleInfo;
```
## Detailed Description
The  structure is used by pg_dump to manage PostgreSQL rules, which are stored in the pg_rewrite system catalog. Rules in PostgreSQL define query rewrite transformations, most commonly used to implement views (which have ON SELECT rules) but can also define custom INSERT, UPDATE, or DELETE transformations on tables. The structure stores metadata about each rule to enable proper dumping and restoration, including the event type, whether it's an INSTEAD rule, and its enabled status.

Special handling is required for view rules (ON SELECT rules) which need to be dumped before the view definition itself, while other rules are dumped after their associated table.

## Parameters / Member Variables
- `dobj`: Base DumpableObject structure containing common dump object metadata (object type DO_RULE, catalog ID, dump ID, name, namespace)
- `*ruletable`: Pointer to the TableInfo structure representing the table or view that the rule is defined on
- `ev_type`: Event type character ('1' for SELECT, '2' for UPDATE, '3' for INSERT, '4' for DELETE)
- `is_instead`: Boolean indicating whether this is an INSTEAD rule (replaces the original query rather than supplementing it)
- `ev_enabled`: Rule enabled status character ('O' for origin, 'D' for disabled, 'R' for replica, 'A' for always)
- `separate`: Boolean indicating whether the rule must be dumped as a separate item (always true for non-ON SELECT rules)
## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - [TableInfo](../T/TableInfo.md) (for table association)
  
- Called from (representative examples):
  - [getRules](../g/getRules.md)() (creates RuleInfo objects by querying pg_rewrite system catalog)
  - [dumpRule](../d/dumpRule.md)() (generates CREATE RULE SQL statements during dump)
  - Dependency sorting functions in pg_dump_sort.c (for handling view rule ordering)

## Notes and Other Information
- The structure is allocated as an array using pg_malloc() in getRules()
- Objects of this type have objType set to DO_RULE
- View rules (ev_type='1' and is_instead=true) receive special treatment in dependency sorting to ensure they are dumped before the view definition
- The separate field controls whether the rule is dumped as an independent object or inline with the table definition
- Event type mapping: '1'=SELECT, '2'=UPDATE, '3'=INSERT, '4'=DELETE
- Enabled status affects whether the rule is active and in what contexts (origin/replica/always/disabled)
- Used exclusively within the pg_dump utility for handling PostgreSQL rule objects