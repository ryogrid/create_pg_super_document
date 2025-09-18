# relation_needs_vacanalyze

## Location
[src/backend/postmaster/autovacuum.c:2942-3117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L2942-L3117)

## Overview
Determines whether a relation needs to be vacuumed or analyzed based on tuple statistics, thresholds, and wraparound protection requirements.

## Definition


## Detailed Description
This function is a core decision-making component of PostgreSQL's autovacuum system. It analyzes various statistics and thresholds to determine if a table requires vacuuming or analyzing. The function implements PostgreSQL's autovacuum algorithm by:

1. **Threshold-based Analysis**: Calculates vacuum and analyze thresholds using base thresholds plus scale factors multiplied by relation tuple counts
2. **Wraparound Protection**: Forces vacuum when transaction ID or MultiXactId age approaches dangerous limits (freeze_max_age)
3. **Configuration Integration**: Uses table-specific reloptions when available, falling back to GUC variables
4. **Statistics Evaluation**: Compares dead tuples, inserted tuples, and modified tuples against calculated thresholds

The vacuum threshold formula is: 
The analyze threshold uses similar logic for tuples modified since last analyze.

## Parameters / Member Variables
- : OID of the relation being evaluated
- : Autovacuum options from table's reloptions (NULL if using defaults)
- : pg_class catalog entry containing relation metadata
- : Statistics entry from pgstats (NULL if no stats available)
- : Maximum age for multixact freeze decisions
- : (Output) Whether vacuum is needed
- : (Output) Whether analyze is needed  
- : (Output) Whether vacuum is forced due to wraparound risk

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsNormal
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)  
  - MultiXactIdIsValid
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md)
  - [AutoVacuumingActive](../A/AutoVacuumingActive.md)
  - PointerIsValid
- Called from (representative examples):
  - [do_autovacuum](../d/do_autovacuum.md)
  - [recheck_relation_needs_vacanalyze](recheck_relation_needs_vacanalyze.md)

## Notes and Other Information
- Automatically skips relations with autovacuum_enabled=false unless wraparound protection is needed
- Special handling for pg_statistic relation (never analyzed)
- Supports insert-based vacuum thresholds in addition to traditional dead-tuple thresholds  
- Falls back to GUC defaults when reloptions are not specified (-1 values)
- Critical for preventing transaction ID wraparound which would cause database shutdown