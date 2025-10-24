# dumpTransform

## Location
[src/bin/pg_dump/pg_dump.c:12833-12961](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L12833-L12961)

## Overview
Generates SQL DDL statements to recreate PostgreSQL transforms, which define how data types are converted between SQL and procedural language representations.

## Definition
```c
static void dumpTransform(Archive *fout, const TransformInfo *transform)
```

## Detailed Description
This function creates SQL CREATE TRANSFORM statements to recreate type transforms in PostgreSQL. Transforms define conversion functions between SQL data types and their representations in procedural languages (like PL/Python, PL/Perl, etc.). The function handles both directions of transformation: FROM SQL (converting SQL values to language-specific representations) and TO SQL (converting language values back to SQL types). Each transform can specify one or both conversion functions.

Key responsibilities include:
- Building CREATE TRANSFORM statements with appropriate function specifications
- Handling bidirectional transformations (FROM SQL and TO SQL functions)
- Resolving and validating transform function definitions
- Generating corresponding DROP TRANSFORM statements for clean replacements
- Managing transform comments and binary upgrade scenarios

## Parameters / Member Variables
- `fout`: Archive structure containing dump context and output formatting information
- `transform`: TransformInfo structure containing transform metadata including type OID, language OID, and function OIDs for both conversion directions

## Dependencies
- Functions called/Symbols referenced:
  - [findFuncByOid](../f/findFuncByOid.md)
  - [get_language_name](../g/get_language_name.md)
  - [getFormattedTypeName](../g/getFormattedTypeName.md)
  - [format_function_signature](../f/format_function_signature.md)
  - [fmtId](../f/fmtId.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - pg_log_warning
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)
  - fmtQualifiedDumpable

## Notes and Other Information
- The function is skipped entirely during data-only dumps (when dopt->dataOnly is true)
- Transforms require at least one function (FROM SQL or TO SQL) to be valid
- Both transform functions are validated for existence before generating SQL
- Function names in transform definitions are always namespace-qualified for clarity
- Error handling includes warnings for missing function definitions or invalid transform configurations
- Transform dependencies are properly tracked and included in the archive entry
- Memory management uses PostgreSQL's PQExpBuffer system with proper cleanup
- Binary upgrade scenarios are supported through extension member handling
- The function handles the comma separation logic when both FROM SQL and TO SQL functions are present

## Simplified Source

```c
static void
dumpTransform(Archive *fout, const TransformInfo *transform)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer defqry, delqry, labelq, transformargs;
    FuncInfo *fromsqlFuncInfo = NULL, *tosqlFuncInfo = NULL;
    char *lanname;
    const char *transformType;

    // Skip in data-only dumps
    if (dopt->dataOnly)
        return;

    // Find transform functions if they exist
    if (OidIsValid(transform->trffromsql)) {
        fromsqlFuncInfo = findFuncByOid(transform->trffromsql);
        if (fromsqlFuncInfo == NULL)
            pg_fatal("could not find function definition for function with OID %u",
                     transform->trffromsql);
    }

    if (OidIsValid(transform->trftosql)) {
        tosqlFuncInfo = findFuncByOid(transform->trftosql);
        if (tosqlFuncInfo == NULL)
            pg_fatal("could not find function definition for function with OID %u",
                     transform->trftosql);
    }

    // Initialize buffers
    defqry = createPQExpBuffer();
    delqry = createPQExpBuffer();
    labelq = createPQExpBuffer();
    transformargs = createPQExpBuffer();

    // Get language and type names
    lanname = get_language_name(fout, transform->trflang);
    transformType = getFormattedTypeName(fout, transform->trftype, zeroAsNone);

    // Build DROP statement
    appendPQExpBuffer(delqry, "DROP TRANSFORM FOR %s LANGUAGE %s;\n",
                      transformType, lanname);

    // Build CREATE TRANSFORM statement
    appendPQExpBuffer(defqry, "CREATE TRANSFORM FOR %s LANGUAGE %s (",
                      transformType, lanname);

    // Validate at least one function exists
    if (!transform->trffromsql && !transform->trftosql)
        pg_log_warning("bogus transform definition, at least one of trffromsql and trftosql should be nonzero");

    // Add FROM SQL function
    if (transform->trffromsql) {
        if (fromsqlFuncInfo) {
            char *fsig = format_function_signature(fout, fromsqlFuncInfo, true);
            appendPQExpBuffer(defqry, "FROM SQL WITH FUNCTION %s.%s",
                              fmtId(fromsqlFuncInfo->dobj.namespace->dobj.name), fsig);
            free(fsig);
        } else {
            pg_log_warning("bogus value in pg_transform.trffromsql field");
        }
    }

    // Add TO SQL function
    if (transform->trftosql) {
        if (transform->trffromsql)
            appendPQExpBufferStr(defqry, ", ");

        if (tosqlFuncInfo) {
            char *fsig = format_function_signature(fout, tosqlFuncInfo, true);
            appendPQExpBuffer(defqry, "TO SQL WITH FUNCTION %s.%s",
                              fmtId(tosqlFuncInfo->dobj.namespace->dobj.name), fsig);
            free(fsig);
        } else {
            pg_log_warning("bogus value in pg_transform.trftosql field");
        }
    }

    appendPQExpBufferStr(defqry, ");\n");

    // Prepare label and arguments for archiving
    appendPQExpBuffer(labelq, "TRANSFORM FOR %s LANGUAGE %s", transformType, lanname);
    appendPQExpBuffer(transformargs, "FOR %s LANGUAGE %s", transformType, lanname);

    // Handle binary upgrade
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(defqry, &transform->dobj, "TRANSFORM",
                                        transformargs->data, NULL);

    // Archive the transform
    if (transform->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, transform->dobj.catId, transform->dobj.dumpId,
                     ARCHIVE_OPTS(.tag = labelq->data,
                                  .description = "TRANSFORM",
                                  .section = SECTION_PRE_DATA,
                                  .createStmt = defqry->data,
                                  .dropStmt = delqry->data,
                                  .deps = transform->dobj.dependencies,
                                  .nDeps = transform->dobj.nDeps));

    // Dump transform comments
    if (transform->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "TRANSFORM", transformargs->data, NULL, "",
                    transform->dobj.catId, 0, transform->dobj.dumpId);

    // Cleanup
    free(lanname);
    destroyPQExpBuffer(defqry);
    destroyPQExpBuffer(delqry);
    destroyPQExpBuffer(labelq);
    destroyPQExpBuffer(transformargs);
}
```