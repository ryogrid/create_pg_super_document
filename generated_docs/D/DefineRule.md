# DefineRule

## Location
src/backend/rewrite/rewriteDefine.c: 190 - 223

## Overview
DefineRule is the main entry point for executing CREATE RULE commands, serving as a high-level interface that coordinates rule parsing, relation locking, and rule definition.

## Definition


## Detailed Description
DefineRule acts as the primary interface for CREATE RULE command execution in PostgreSQL. It performs the initial parsing and transformation of the rule statement through transformRuleStmt, acquires the necessary locks on the target relation, and then delegates the actual rule creation to DefineQueryRewrite. This function bridges the gap between the parser's output (RuleStmt) and the lower-level rule definition machinery, ensuring proper statement transformation and relation access before rule creation.

## Parameters / Member Variables
- : The parsed CREATE RULE statement containing rule name, target relation, event type, conditions, and actions
- : The original SQL command string for error reporting and logging purposes

## Dependencies
- Functions called/Symbols referenced:
  - transformRuleStmt
  - RangeVarGetRelid
  - AccessExclusiveLock
  - DefineQueryRewrite
- Called from (representative examples):
  - ProcessUtilitySlow

## Notes and Other Information
- This function represents the public interface for rule creation and is called by the utility command processor
- Uses AccessExclusiveLock to match the locking level used by DefineQueryRewrite for consistency
- The function performs minimal processing itself, acting primarily as a coordinator between parsing and rule definition phases
- Returns an ObjectAddress identifying the newly created rule object
- Part of the DDL (Data Definition Language) command processing infrastructure