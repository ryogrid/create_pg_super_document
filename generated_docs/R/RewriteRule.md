# RewriteRule

## Location
[src/include/rewrite/prs2lock.h:24-32](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/rewrite/prs2lock.h#L24-L32)

## Overview
RewriteRule is a structure that holds information for a rewrite rule in PostgreSQL's rule system, which is used for implementing views, query rewriting, and other rule-based transformations.

## Definition

```c
typedef struct RewriteRule
{
	Oid			ruleId;
	CmdType		event;
	Node	   *qual;
	List	   *actions;
	char		enabled;
	bool		isInstead;
} RewriteRule;
```
## Detailed Description
RewriteRule represents a single rewrite rule within PostgreSQL's rule system. These rules are used to transform queries, particularly for implementing views and handling certain types of query modifications. The structure contains all the essential information needed to identify, evaluate, and execute a rewrite rule during query processing.

The rule system allows PostgreSQL to automatically rewrite queries based on predefined rules. This is the fundamental mechanism behind views, where SELECT queries on views are rewritten to query the underlying base tables.

## Parameters / Member Variables
- : The object identifier (OID) that uniquely identifies this rule in the system catalogs
- : The type of command that triggers this rule (SELECT, INSERT, UPDATE, DELETE)
- : A qualification (WHERE clause) that determines when the rule should be applied; NULL if always applicable
- : A list of actions (queries) to be executed when the rule fires
- : Character indicating whether the rule is enabled ('O' for origin, 'R' for replica, 'A' for always, 'D' for disabled)
- : Boolean flag indicating whether this is an INSTEAD rule (replaces the original query) or an ALSO rule (executes in addition)

## Dependencies
- Functions called/Symbols referenced:
  - CmdType
  - [Node](../N/Node.md)
  - [List](../L/List.md)
  - Oid

- Called from (representative examples):
  - [RelationBuildRuleLock](RelationBuildRuleLock.md)
  - [fireRules](../f/fireRules.md)
  - [ApplyRetrieveRule](../A/ApplyRetrieveRule.md)
  - [DefineQueryRewrite](../D/DefineQueryRewrite.md)
  - [get_view_query](../g/get_view_query.md)

## Notes and Other Information
- [RewriteRule](RewriteRule.md) structures are typically stored within RuleLock structures, which contain arrays of rules for a relation
- The rule system is a core component of PostgreSQL's query processing pipeline
- Views in PostgreSQL are implemented using the rule system with SELECT rules
- The enabled field supports different replication scenarios where rules may be applied differently on master vs replica servers
- INSTEAD rules completely replace the triggering query, while ALSO rules execute additional actions alongside the original query