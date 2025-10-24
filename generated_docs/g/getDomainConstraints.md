# getDomainConstraints

## Location
[src/bin/pg_dump/pg_dump.c:8010-8123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L8010-L8123)

## Overview
Retrieves and processes constraint information for PostgreSQL domains, including CHECK and NOT NULL constraints, preparing them for dump operations.

## Definition

```c
static void
getDomainConstraints(Archive *fout, TypeInfo *tyinfo)
```
## Detailed Description
This function queries the PostgreSQL system catalog to obtain constraint information for a specific domain type. It handles both CHECK constraints and NOT NULL constraints (for PostgreSQL 17+), preparing them for inclusion in database dumps. The function uses prepared statements for efficient querying and creates ConstraintInfo structures to represent each constraint.

The function differentiates between validated and unvalidated constraints, with unvalidated constraints being marked for separate dumping. For validated constraints, it establishes dependency relationships to ensure proper restoration order. The function also distinguishes between CHECK constraints (stored in an array) and NOT NULL constraints (stored as a single reference).

Key behaviors include:
- Using prepared statements for efficient repeated queries
- Supporting version-specific constraint types (NOT NULL constraints added in PostgreSQL 17)
- Managing constraint validation status and dump ordering
- Creating proper dependency relationships between domains and their constraints

## Parameters / Member Variables
- `*fout`: Archive pointer representing the dump context and connection information
- `*tyinfo`: TypeInfo structure containing information about the domain type being processed
## Dependencies
- Functions called/Symbols referenced:
  - [TypeInfo](../T/TypeInfo.md) (struct type)
  - [ConstraintInfo](../C/ConstraintInfo.md) (struct type)
  - [createPQExpBuffer](../c/createPQExpBuffer.md) (function)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) (function)
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md) (function)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (function)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md) (function)
  - [PQntuples](../P/PQntuples.md), PQfnumber, PQgetvalue (libpq functions)
  - [pg_malloc](../p/pg_malloc.md) (memory allocation)
  - [AssignDumpId](../A/AssignDumpId.md) (function)
  - [addObjectDependency](../a/addObjectDependency.md) (function)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md) (function)
  - CONSTRAINT_CHECK, CONSTRAINT_NOTNULL (enum values)
  - DO_CONSTRAINT (enum value)

- Called from (representative examples):
  - [getTypes](getTypes.md) (primary caller during domain type processing)

## Notes and Other Information
- This is a static function accessible only within pg_dump.c
- Supports version-specific behavior: PostgreSQL 17+ includes NOT NULL constraints ('n' type) in addition to CHECK constraints ('c' type)
- Uses prepared statements (PREPQUERY_GETDOMAINCONSTRAINTS) for performance optimization
- Manages constraint validation status - unvalidated constraints are dumped separately to avoid restoration issues
- Creates proper dependency chains to ensure domains are not restored before their constraint dependencies are satisfied
- Handles both array-based storage for CHECK constraints and single reference storage for NOT NULL constraints
- The function assumes domains can have at most one NOT NULL constraint

## Simplified Source

```c
static void getDomainConstraints(Archive *fout, TypeInfo *tyinfo)
{
    ConstraintInfo *constrinfo;
    PQExpBuffer query = createPQExpBuffer();
    PGresult   *res;
    int         ntups;

    // Prepare the query if not already done
    if (!fout->is_prepared[PREPQUERY_GETDOMAINCONSTRAINTS])
    {
        // Build version-specific query for domain constraints
        // v17+ supports NOT NULL constraints ('n') in addition to CHECK ('c')
        appendPQExpBuffer(query,
            "PREPARE getDomainConstraints(pg_catalog.oid) AS "
            "SELECT tableoid, oid, conname, "
            "pg_catalog.pg_get_constraintdef(oid) AS consrc, "
            "convalidated, contype "
            "FROM pg_catalog.pg_constraint "
            "WHERE contypid = $1 AND contype IN (%s) "
            "ORDER BY conname",
            fout->remoteVersion < 170000 ? "'c'" : "'c', 'n'");

        ExecuteSqlStatement(fout, query->data);
        fout->is_prepared[PREPQUERY_GETDOMAINCONSTRAINTS] = true;
    }

    // Execute the prepared statement for this domain
    printfPQExpBuffer(query, "EXECUTE getDomainConstraints('%u')", tyinfo->dobj.catId.oid);
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);

    // Extract column indices
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_oid = PQfnumber(res, "oid");
    int i_conname = PQfnumber(res, "conname");
    int i_consrc = PQfnumber(res, "consrc");
    int i_convalidated = PQfnumber(res, "convalidated");
    int i_contype = PQfnumber(res, "contype");

    constrinfo = (ConstraintInfo *) pg_malloc(ntups * sizeof(ConstraintInfo));
    tyinfo->domChecks = constrinfo;

    // Process each constraint
    for (int i = 0, j = 0; i < ntups; i++)
    {
        bool validated = PQgetvalue(res, i, i_convalidated)[0] == 't';
        char contype = (PQgetvalue(res, i, i_contype))[0];
        ConstraintInfo *constraint;

        // Handle constraint type-specific storage
        if (contype == CONSTRAINT_CHECK)
        {
            constraint = &constrinfo[j++];
            tyinfo->nDomChecks++;
        }
        else  // NOT NULL constraint (v17+)
        {
            Assert(contype == CONSTRAINT_NOTNULL);
            Assert(tyinfo->notnull == NULL);
            tyinfo->notnull = &(constrinfo[ntups - 1]);
            constraint = tyinfo->notnull;
        }

        // Initialize constraint object
        constraint->dobj.objType = DO_CONSTRAINT;
        constraint->dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        constraint->dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&constraint->dobj);

        // Set constraint properties
        constraint->dobj.name = pg_strdup(PQgetvalue(res, i, i_conname));
        constraint->dobj.namespace = tyinfo->dobj.namespace;
        constraint->contable = NULL;
        constraint->condomain = tyinfo;
        constraint->contype = contype;
        constraint->condef = pg_strdup(PQgetvalue(res, i, i_consrc));
        constraint->conislocal = true;

        // Unvalidated constraints are dumped separately
        constraint->separate = !validated;

        // Create dependency from domain to validated constraints
        if (validated)
            addObjectDependency(&tyinfo->dobj, constraint->dobj.dumpId);
    }

    PQclear(res);
    destroyPQExpBuffer(query);
}
```