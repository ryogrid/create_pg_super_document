# make_restrictinfo_internal

## Location
src/backend/optimizer/util/restrictinfo.c: 112 - 270

## Overview
Internal common implementation for creating RestrictInfo nodes, handling the detailed initialization of all RestrictInfo fields including relation dependencies, join capabilities, and performance optimization caches.

## Definition


## Detailed Description
This static function serves as the core implementation for RestrictInfo creation, performing comprehensive initialization of all RestrictInfo fields. It analyzes the clause structure to determine relation dependencies, evaluates join potential for binary operator clauses, handles security considerations including leak-proofness testing, and initializes performance-related caches. The function distinguishes between binary operator clauses (which may be join-capable) and other clause types, setting up appropriate left/right relation information for optimization purposes.

## Parameters / Member Variables
- : PlannerInfo structure containing planning context and global state
- : The primary expression being wrapped in the RestrictInfo
- : Parent OR clause if this RestrictInfo is part of an OR structure (can be NULL)
- : Flag indicating whether this restriction was pushed down from a higher query level
- : Flag indicating whether this RestrictInfo has associated clones
- : Flag indicating whether this RestrictInfo is itself a clone of another
- : Flag indicating whether the clause evaluates to a constant value
- : Security level for row-level security evaluation ordering
- : Explicit set of required relations (defaults to clause_relids if NULL)
- : Set of relations incompatible with this restriction
- : Set of relations that are outer to this restriction context

## Dependencies
- Functions called/Symbols referenced:
  - contain_leaked_vars
  - VOLATILITY_UNKNOWN
  - is_opclause
  - OpExpr
  - get_leftop
  - get_rightop
  - pull_varnos
  - bms_union
  - bms_is_empty
  - bms_overlap
  - bms_difference
  - bms_num_members
  - bms_free
- Called from (representative examples):
  - make_restrictinfo
  - make_sub_restrictinfos

## Notes and Other Information
- Special handling for binary operator clauses: analyzes left and right operands separately to determine join capability and relation dependencies
- Security considerations: tests for leak-proofness when security_level > 0 to support row-level security
- Performance optimization: initializes numerous cache fields with sentinel values (-1, NIL, InvalidOid) that will be populated on-demand during query optimization
- Join detection: automatically identifies potential join clauses by checking if left and right operands reference disjoint sets of relations
- Base relation counting: calculates the number of base relations involved by excluding outer join relations from the clause's relation set
- Serial numbering: assigns a unique serial number to each RestrictInfo for debugging and tracking purposes
- Lazy evaluation design: most expensive computations (selectivity, join costs, etc.) are deferred until actually needed during optimization