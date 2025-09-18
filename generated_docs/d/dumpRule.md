# dumpRule

## Location
src/bin/pg_dump/pg_dump.c: 18104 - 18270

## Overview
Dumps PostgreSQL rules, with special handling for view-defining ON SELECT rules that are treated as CREATE VIEW statements rather than separate rule objects.

## Definition


## Detailed Description
The  function generates SQL statements for PostgreSQL rules, with sophisticated logic to handle different rule types. For ON SELECT rules that define views (ev_type == '1' and is_instead == true), it generates CREATE OR REPLACE VIEW statements instead of CREATE RULE statements, including view options and CHECK OPTION clauses. For regular rules, it uses pg_get_ruledef() to retrieve the complete rule definition. The function also handles rule replication firing semantics through ALTER TABLE ENABLE/DISABLE RULE commands when the rule's enabled state differs from the default ('O'). Non-separate rules (typically implicit view rules) are skipped entirely.

## Parameters / Member Variables
- : Archive structure containing dump options and output methods  
- : RuleInfo structure containing rule metadata including rule table, event type, instead flag, enabled state, and separation flag

## Dependencies
- Functions called/Symbols referenced:
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [nonemptyReloptions](../n/nonemptyReloptions.md)
  - [appendReloptionsArrayAH](../a/appendReloptionsArrayAH.md)
  - [createViewAsClause](../c/createViewAsClause.md)
  - [createDummyViewAsClause](../c/createDummyViewAsClause.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [psprintf](../p/psprintf.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Skips processing in data-only dump mode and for non-separate rules
- Distinguishes between view-defining rules (ON SELECT INSTEAD) and regular rules
- For views, uses CREATE OR REPLACE VIEW to handle dummy view replacement during restore
- Handles three rule enabled states: 'A' (always), 'R' (replica), 'D' (disabled), with 'O' being default
- Creates archive entries in SECTION_POST_DATA to ensure rules are created after their dependent tables
- For view rules, DROP statements use CREATE OR REPLACE VIEW with dummy content instead of DROP RULE
- Preserves view reloptions and check options when dumping view-defining rules