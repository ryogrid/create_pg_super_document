# print_rel_infos

## Location
[src/bin/pg_upgrade/info.c:813-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/info.c#L813-L825)

## Overview
Prints detailed information about all relations in a RelInfoArr structure for debugging purposes during pg_upgrade.

## Definition
```c
static void print_rel_infos(RelInfoArr *rel_arr)
```

## Detailed Description
This function provides verbose logging output for relation information during the pg_upgrade process. It iterates through all relations in the provided RelInfoArr and logs detailed information about each relation including the namespace-qualified relation name, relation OID, and tablespace. The output format shows each relation as "namespace.relation_name" along with its unique identifier and storage location. This debugging information helps administrators monitor which relations are being processed during the upgrade and verify their metadata.

## Parameters / Member Variables
- `rel_arr`: Pointer to RelInfoArr structure containing relation information to be printed

## Dependencies
- Functions called/Symbols referenced:
  - [pg_log](pg_log.md)
  - RelInfoArr (struct type)
  - PG_VERBOSE (log level constant)
- Called from (representative examples):
  - [print_db_infos](print_db_infos.md)

## Notes and Other Information
- This is a static function only used within src/bin/pg_upgrade/info.c
- Uses PG_VERBOSE logging level, so output is only visible when verbose logging is enabled
- Output format: "relname: "namespace.relation", reloid: OID, reltblspace: "tablespace""
- [Relation](../R/Relation.md) names and tablespace names are quoted in the output for clarity
- Part of the pg_upgrade utility's debugging and monitoring system for relation metadata
- Provides essential information for troubleshooting upgrade issues related to specific relations

## Simplified Source

```c
static void
print_rel_infos(RelInfoArr *rel_arr)
{
    // Print detailed information for each relation
    for (int relnum = 0; relnum < rel_arr->nrels; relnum++)
        pg_log(PG_VERBOSE, "relname: \"%s.%s\", reloid: %u, reltblspace: \"%s\"",
               rel_arr->rels[relnum].nspname,
               rel_arr->rels[relnum].relname,
               rel_arr->rels[relnum].reloid,
               rel_arr->rels[relnum].tablespace);
}
```