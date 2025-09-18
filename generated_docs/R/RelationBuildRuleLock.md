# RelationBuildRuleLock

## Location
src/backend/utils/cache/relcache.c: 733 - 907

## Overview
RelationBuildRuleLock constructs the rewrite rule structures for a relation by scanning pg_rewrite and parsing rule definitions into executable form.

## Definition
static void RelationBuildRuleLock(Relation relation)

## Detailed Description
RelationBuildRuleLock builds the complete rule lock structure (rd_rules) for a relation by scanning the pg_rewrite system catalog and constructing parsed rule trees. The function creates a dedicated memory context for rule data to enable efficient cleanup when the relcache entry is flushed. It processes each rule by extracting the rule definition strings, parsing them into node trees, and setting up appropriate security context information based on view security settings.

The function handles the complex task of converting textual rule representations back into executable parse trees, managing memory carefully to avoid leaks from large rule strings that may be TOASTed. It also implements security invoker semantics for views by setting appropriate checkAsUser fields in the rule trees.

## Parameters / Member Variables
- : The relation descriptor whose rules (rd_rules) will be populated from pg_rewrite catalog data

## Dependencies
- Functions called/Symbols referenced:
  - SysScanDesc, RuleLock, RewriteRule (data structure types)
  - AllocSetContextCreate, ALLOCSET_SMALL_SIZES (memory context creation)
  - MemoryContextCopyAndSetIdentifier, MemoryContextAlloc (memory management)
  - systable_beginscan, systable_getnext (system catalog scanning)
  - Form_pg_rewrite (pg_rewrite tuple structure type)
  - heap_getattr (attribute extraction from tuples)
  - TextDatumGetCString, stringToNode (text parsing and node tree construction)
  - CMD_SELECT, RELKIND_VIEW (constants for rule type checking)
  - RelationHasSecurityInvoker (security invoker view detection)
  - setRuleCheckAsUser (security context setup for rule trees)
  - repalloc, MemoryContextDelete (memory management utilities)
- Called from (representative examples):
  - RelationBuildDesc (during complete relation descriptor construction)

## Notes and Other Information
- Creates a dedicated memory context (rd_rulescxt) for rule data to enable efficient cleanup
- Scans rules in name order using RewriteRelRulenameIndexId for consistent rule firing order
- Handles TOAST decompression for large rule strings and manages memory carefully to prevent leaks
- Implements security invoker semantics for SELECT rules on security_invoker views
- Sets checkAsUser fields in rule trees during loading rather than storage for ALTER TABLE OWNER efficiency
- Dynamically expands rule array as needed with repalloc when maxlocks is exceeded
- Handles the case where relhasrules may be out-of-date by checking for zero rules
- Parses both rule actions and qualifications from textual representations into node trees
- Essential for view and rule functionality in PostgreSQL's query rewrite system