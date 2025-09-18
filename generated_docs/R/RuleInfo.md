# RuleInfo

## Location
[src/bin/pg_dump/pg_dump.h:451-452](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L451-L452)

## Overview
RuleInfo represents database rules in PostgreSQL's pg_dump utility, containing metadata about rule definitions, event types, and dumping behavior for rule objects.

## Definition


## Detailed Description
RuleInfo is a data structure in pg_dump that represents PostgreSQL rules during the database dumping process. Rules in PostgreSQL are a powerful mechanism for query rewriting that can transform queries on-the-fly, commonly used to implement updatable views and complex query transformations.

The structure captures essential metadata about rules including their event type (SELECT, INSERT, UPDATE, DELETE), whether they are INSTEAD rules (which replace the original query), their enabled status, and how they should be handled during the dump process. The separate flag is particularly important as it determines whether the rule should be dumped as a standalone CREATE RULE statement or integrated with other objects.

Rules are closely associated with PostgreSQL's view system, where ON SELECT rules are automatically created for views and need special handling during dump and restore operations to maintain proper dependencies and avoid circular references.

## Parameters / Member Variables
- : Base DumpableObject containing common metadata like catalog ID, dump ID, name, namespace, and dependencies
- : Pointer to the TableInfo structure representing the table or view this rule is associated with
- : Character indicating the event type that triggers this rule ('1'=SELECT, '2'=UPDATE, '3'=INSERT, '4'=DELETE)
- : Boolean indicating whether this is an INSTEAD rule that replaces the triggering query rather than supplementing it
- : Character indicating the rule's enabled status ('O'=origin, 'D'=disabled, 'R'=replica, 'A'=always)
- : Boolean flag determining if the rule must be dumped as a separate CREATE RULE statement (always true for non-SELECT rules)

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (inherited base structure)
  - [TableInfo](../T/TableInfo.md) (referenced via ruletable pointer)
- Called from (representative examples):
  - [getRules](../g/getRules.md) (creates and populates RuleInfo structures)
  - [dumpRule](../d/dumpRule.md) (generates CREATE RULE commands)
  - [repairViewRuleMultiLoop](../r/repairViewRuleMultiLoop.md) (handles circular dependencies in view rules)
  - [repairDependencyLoop](../r/repairDependencyLoop.md) (resolves rule-related dependency cycles)
  - [addBoundaryDependencies](../a/addBoundaryDependencies.md) (manages rule dependencies)

## Notes and Other Information
- [RuleInfo](RuleInfo.md) objects are created during schema discovery by getRules()
- ON SELECT rules for views are typically not dumped separately but are recreated implicitly with CREATE VIEW
- INSTEAD rules are commonly used to make views updatable by defining how INSERT/UPDATE/DELETE operations should be handled
- The ev_enabled field supports PostgreSQL's rule enabling/disabling mechanism for replication scenarios
- Rules can create complex dependency relationships that require special handling during dump ordering
- Non-SELECT rules (INSERT, UPDATE, DELETE) always have separate=true to ensure proper dump ordering
- Rules are part of PostgreSQL's query rewrite system and can significantly impact query execution plans
- The structure supports PostgreSQL's sophisticated rule system used for implementing views, security policies, and query transformations