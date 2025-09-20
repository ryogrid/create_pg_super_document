# scanNameSpaceForRefname

## Location
[src/backend/parser/parse_relation.c:200-239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L200-L239)

## Overview
Searches the current parsing state's namespace for an item matching an unqualified reference name, handling ambiguity detection and lateral reference validation.

## Definition

```c
static ParseNamespaceItem *
scanNameSpaceForRefname(ParseState *pstate, const char *refname, int location)
```
## Detailed Description
This static function scans through the p_namespace list in the current parsing state to find a namespace item that matches the given unqualified reference name. It implements PostgreSQL's relaxed alias scoping rules, allowing certain cases of duplicate aliases that would be forbidden in strict SQL. The function checks for ambiguous references and reports errors when multiple visible items match the same name. It also handles lateral-only items and performs lateral reference validation.

## Parameters / Member Variables
- : Current parsing state containing the namespace list to search
- : The unqualified name to search for (table alias or relation name)
- : Source location in the query for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [check_lateral_ref_ok](../c/check_lateral_ref_ok.md)
  - ereport (for error reporting)
  - strcmp (for name comparison)
- Called from (representative examples):
  - [refnameNamespaceItem](../r/refnameNamespaceItem.md)

## Notes and Other Information
- Only considers items with p_rel_visible set to true (ignores columns-only items)
- Respects lateral scoping rules by checking p_lateral_only and p_lateral_active flags
- Reports ERRCODE_AMBIGUOUS_ALIAS when multiple items match the same name
- Part of PostgreSQL's namespace resolution system that allows more flexible alias usage than standard SQL
- Implements historical PostgreSQL behavior for backward compatibility