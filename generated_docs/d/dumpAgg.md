# dumpAgg

## Location
[src/bin/pg_dump/pg_dump.c:14227-14586](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L14227-L14586)

## Overview
Writes out a single aggregate function definition, generating CREATE AGGREGATE SQL statements with all necessary parameters including state functions, final functions, parallel options, and moving aggregates support.

## Definition

```c
static void
dumpAgg(Archive *fout, const AggInfo *agginfo)
```
## Detailed Description
The  function generates SQL commands to recreate aggregate functions during database dumps. It handles the complexity of PostgreSQL's aggregate definition syntax by constructing comprehensive CREATE AGGREGATE statements with all supported options:

- **Basic components**: State function (SFUNC), state type (STYPE), final function (FINALFUNC)
- **Advanced features**: Combine/serialize/deserialize functions for parallel aggregation
- **Moving aggregates**: Forward/inverse state functions (MSFUNC/MINVFUNC) for window functions
- **Optimization settings**: Parallel safety, state space estimation, function modify behavior
- **Special aggregate types**: Hypothetical aggregates, ordered-set aggregates

The function uses prepared statements for efficiency and includes extensive version compatibility handling across PostgreSQL 9.4+, 9.6+, and 11.0+ to manage evolving aggregate features. It processes aggregate metadata from pg_aggregate and pg_proc catalogs to generate complete aggregate definitions.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump options and output methods
- `*agginfo`: AggInfo structure containing aggregate function metadata including OID, name, namespace, owner, and function details
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)  
  - [format_function_arguments](../f/format_function_arguments.md)
  - [format_aggregate_signature](../f/format_aggregate_signature.md)
  - [format_function_signature](../f/format_function_signature.md)
  - [getFormattedOperatorName](../g/getFormattedOperatorName.md)
  - appendStringLiteralAH
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Only operates in schema dump mode (skipped when dopt->dataOnly is true)
- Uses prepared statements (PREPQUERY_DUMPAGG) for query optimization
- Handles version differences for aggregate features introduced over time
- Generates both identity signatures for DROP and full signatures for CREATE statements
- Special handling for ACL dumps using function syntax (no native GRANT ON AGGREGATE)
- Supports binary upgrade scenarios with extension membership
- Includes comprehensive validation and error handling for aggregate parameters
- Manages complex parameter combinations for different aggregate types and PostgreSQL versions