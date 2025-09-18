# IsDefinedRewriteRule

## Location
src/backend/rewrite/rewriteSupport.c: 32 - 52

## Overview
Checks whether a rewrite rule with a given name exists for a specified relation in the PostgreSQL system catalogs.

## Definition


## Detailed Description
IsDefinedRewriteRule is a utility function that determines if a rewrite rule with the specified name exists for a given relation. It performs this check by searching the system cache for the rule using the RULERELNAME cache, which is indexed by both the relation OID and the rule name. The function returns a boolean value indicating the existence of the rule.

This function is part of PostgreSQL's rewrite rule system, which allows for query transformation and view implementation. Rewrite rules are stored in the pg_rewrite system catalog and cached for efficient access.

## Parameters / Member Variables
- : The OID of the relation (table/view) that owns the rule
- : The name of the rewrite rule to search for

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheExists2
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)  
  - [PointerGetDatum](../P/PointerGetDatum.md)
- Called from (representative examples):
  - [RenameRewriteRule](../R/RenameRewriteRule.md)
  - ViewSelectRuleName

## Notes and Other Information
- The function uses the RULERELNAME system cache for efficient lookup
- Returns true if the rule exists, false otherwise
- This is a read-only operation that does not modify any system state
- Used internally by PostgreSQL's rule management system for validation and lookup operations
- The function is defined in src/backend/rewrite/rewriteSupport.c:32-52