# match_clause_to_index

## Location
src/backend/optimizer/path/indxpath.c: 2084 - 2202

## Overview
Tests whether a qualification clause can be used with an index and adds appropriate IndexClause entries to the clause set if usable.

## Definition
static void match_clause_to_index(PlannerInfo *root, RestrictInfo *rinfo, IndexOptInfo *index, IndexClauseSet *clauseset)

## Detailed Description
This is the core function that determines whether a specific restriction clause (WHERE condition or join condition) can be effectively utilized by a given index. The function performs several validation steps: it rejects pseudoconstant clauses, checks security restrictions using restriction_is_securely_promotable(), and then systematically tests each index key column for compatibility with the clause using match_clause_to_indexcol(). The function includes important optimizations such as duplicate detection (preventing the same RestrictInfo from being added multiple times) and first-match selection (avoiding inflated selectivity estimates when a clause could match multiple index columns). If a match is found, it creates an IndexClause entry and adds it to the appropriate column list in the IndexClauseSet.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global query planning information and context
- `rinfo`: RestrictInfo node representing the qualification clause to be tested
- `index`: IndexOptInfo structure containing detailed information about the index being evaluated
- `clauseset`: IndexClauseSet structure where matching clauses will be stored, organized by index column

## Dependencies
- Functions called/Symbols referenced:
  - restriction_is_securely_promotable
  - match_clause_to_indexcol
  - IndexClause
  - IndexOptInfo
  - IndexClauseSet
- Called from (representative examples):
  - match_join_clauses_to_index
  - match_clauses_to_index
  - ec_member_matches_arg

## Notes and Other Information
- This is a static function, accessible only within the indxpath.c file
- The function includes important security checks to prevent unsafe clause promotion in row-level security scenarios
- Implements duplicate detection using pointer equality to avoid redundant IndexClause entries
- Uses first-match semantics when a clause could potentially match multiple index columns
- The function can handle expression indexes and partial indexes with appropriate safety checks
- Part of PostgreSQL's sophisticated index selection and optimization system
- Location: src/backend/optimizer/path/indxpath.c:2084-2202