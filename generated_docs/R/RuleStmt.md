# RuleStmt

## Location
src/include/nodes/parsenodes.h: 3606 - 3616

## Overview
RuleStmt represents the parsed structure of a CREATE RULE SQL statement that defines rewrite rules for tables and views in PostgreSQL.

## Definition

```c
typedef struct RuleStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* relation the rule is for */
	char	   *rulename;		/* name of the rule */
	Node	   *whereClause;	/* qualifications */
	CmdType		event;			/* SELECT, INSERT, etc */
	bool		instead;		/* is a 'do instead'? */
	List	   *actions;		/* the action statements */
	bool		replace;		/* OR REPLACE */
} RuleStmt;
```
## Detailed Description
RuleStmt is a parse node that represents the CREATE RULE statement in PostgreSQL's SQL grammar. Rules in PostgreSQL are part of the query rewrite system that allows automatic transformation of queries. The statement follows the syntax: `CREATE [OR REPLACE] RULE rulename AS ON event TO table [WHERE condition] DO [INSTEAD] action_list`. This mechanism is primarily used internally for implementing views and can be used to create custom query transformations.

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as a RuleStmt parse node
- `relation`: RangeVar specifying the target table or view for which the rule is defined
- `rulename`: String containing the name of the rule being created
- `whereClause`: Optional WHERE condition that must be satisfied for the rule to fire
- `event`: CmdType specifying the triggering event (SELECT, INSERT, UPDATE, DELETE, etc.)
- `instead`: Boolean flag indicating whether this is a DO INSTEAD rule (replaces the original action)
- `actions`: List of statements to execute when the rule fires
- `replace`: Boolean flag indicating whether this rule should replace an existing rule with the same name

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for type identification)
  - RangeVar (for relation specification)
  - CmdType (for event type specification)
  - List (for actions storage)
- Called from (representative examples):
  - DefineRule (processes the statement in src/backend/rewrite/rewriteDefine.c:190)
  - ProcessUtilitySlow (dispatches the statement in src/backend/tcop/utility.c:1663)
  - transformRuleStmt (transforms the statement in src/backend/parser/parse_utilcmd.c:2967)

## Notes and Other Information
- Rules are parsed in gram.y with the syntax: `CREATE opt_or_replace RULE name AS ON event TO qualified_name where_clause DO opt_instead RuleActionList`
- The rule system is primarily used internally for view implementation but can also be used for custom query transformations
- DO INSTEAD rules completely replace the triggering event, while DO ALSO rules add additional actions
- Rules require AccessExclusiveLock on the target relation during creation
- This is a core component of PostgreSQL's query rewrite system that enables advanced query transformation capabilities