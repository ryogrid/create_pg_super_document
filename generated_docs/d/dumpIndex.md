# dumpIndex

## Location
[src/bin/pg_dump/pg_dump.c:16966-17116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L16966-L17116)

## Overview
Writes out a user-defined index to the dump archive, handling both standalone indexes and constraint-associated indexes with appropriate SQL generation and metadata handling.

## Definition


## Detailed Description
The  function is responsible for dumping user-defined indexes in pg_dump. It generates the necessary SQL statements to recreate the index, including:

1. **Index Creation**: Uses the stored index definition to create the basic CREATE INDEX statement
2. **Clustering Information**: Adds ALTER TABLE ... CLUSTER commands if the index is used for clustering
3. **Statistics Settings**: Generates ALTER INDEX ... SET STATISTICS commands for columns with custom statistics targets
4. **Replica Identity**: Sets replica identity using the index if configured
5. **Constraint Handling**: For constraint-backed indexes, only dumps comments (not the index itself, as it's handled by the constraint)
6. **Extension Dependencies**: Records dependencies on extensions
7. **Partitioned Index Handling**: Avoids generating DROP statements for partitioned index members

The function follows PostgreSQL's dump architecture by creating both creation and deletion statements, then archiving them with appropriate metadata.

## Parameters / Member Variables
- : Archive pointer containing dump options and output context
- : IndxInfo structure containing index metadata including:
  - Index definition SQL
  - Clustering status
  - Statistics column information  
  - Replica identity settings
  - Constraint association
  - Parent index relationship (for partitioning)

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - [fmtId](../f/fmtId.md)  
  - fmtQualifiedDumpable
  - [binary_upgrade_set_pg_class_oids](../b/binary_upgrade_set_pg_class_oids.md)
  - [parsePGArray](../p/parsePGArray.md)
  - [append_depends_on_extension](../a/append_depends_on_extension.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Skips processing entirely in data-only dump mode
- Constraint-backed indexes only have their comments dumped, not the index definition itself
- Partitioned index members cannot be dropped independently, so no DROP statement is generated for them
- Binary upgrade mode requires special handling for object OID preservation
- Index statistics are parsed from PostgreSQL array format and converted to individual ALTER INDEX commands
- The function maintains synchronization with similar code in dumpConstraint for consistency