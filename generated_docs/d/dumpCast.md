# dumpCast

## Location
[src/bin/pg_dump/pg_dump.c:12728-12832](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L12728-L12832)

## Overview
Generates SQL DDL statements to recreate a PostgreSQL user-defined cast, handling different cast methods and contexts during database dump operations.

## Definition
```c
static void dumpCast(Archive *fout, const CastInfo *cast)
```

## Detailed Description
This function creates SQL CREATE CAST statements to recreate user-defined type casts in PostgreSQL. It handles three different cast methods: binary casts (direct bit-wise conversion), input/output casts (using type I/O functions), and function-based casts (using explicit conversion functions). The function also manages cast contexts (implicit, assignment, or explicit) and ensures proper qualification of function names when generating SQL output.

Key responsibilities include:
- Building CREATE CAST statements with appropriate method specifications
- Handling different cast contexts (AS ASSIGNMENT, AS IMPLICIT, or explicit)
- Resolving and formatting cast function signatures when applicable
- Generating corresponding DROP CAST statements for clean replacements
- Managing cast comments and binary upgrade scenarios

## Parameters / Member Variables
- `fout`: Archive structure containing dump context and output formatting information
- `cast`: CastInfo structure containing cast metadata including source/target types, cast method, context, and associated function OID

## Dependencies
- Functions called/Symbols referenced:
  - [findFuncByOid](../f/findFuncByOid.md)
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
- Cast methods are distinguished by COERCION_METHOD constants (BINARY, INOUT, FUNCTION)
- Function-based casts require validation that the cast function exists and is accessible
- Cast contexts are represented by single characters: 'a' for assignment, 'i' for implicit
- Function names in cast definitions are always namespace-qualified for clarity
- Error handling includes warnings for invalid cast method values or missing function definitions
- Memory management uses PostgreSQL's PQExpBuffer system with proper cleanup
- Binary upgrade scenarios are supported through extension member handling

## Simplified Source

```c
static void
dumpCast(Archive *fout, const CastInfo *cast)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer defqry, delqry, labelq, castargs;
    FuncInfo *funcInfo = NULL;
    const char *sourceType, *targetType;

    // Skip in data-only dumps
    if (dopt->dataOnly)
        return;

    // Find cast function if one exists
    if (OidIsValid(cast->castfunc)) {
        funcInfo = findFuncByOid(cast->castfunc);
        if (funcInfo == NULL)
            pg_fatal("could not find function definition for function with OID %u",
                     cast->castfunc);
    }

    // Initialize buffers
    defqry = createPQExpBuffer();
    delqry = createPQExpBuffer();
    labelq = createPQExpBuffer();
    castargs = createPQExpBuffer();

    // Get formatted type names
    sourceType = getFormattedTypeName(fout, cast->castsource, zeroAsNone);
    targetType = getFormattedTypeName(fout, cast->casttarget, zeroAsNone);

    // Build DROP statement
    appendPQExpBuffer(delqry, "DROP CAST (%s AS %s);\n", sourceType, targetType);

    // Build CREATE CAST statement
    appendPQExpBuffer(defqry, "CREATE CAST (%s AS %s) ", sourceType, targetType);

    // Add cast method
    switch (cast->castmethod) {
        case COERCION_METHOD_BINARY:
            appendPQExpBufferStr(defqry, "WITHOUT FUNCTION");
            break;
        case COERCION_METHOD_INOUT:
            appendPQExpBufferStr(defqry, "WITH INOUT");
            break;
        case COERCION_METHOD_FUNCTION:
            if (funcInfo) {
                char *fsig = format_function_signature(fout, funcInfo, true);
                appendPQExpBuffer(defqry, "WITH FUNCTION %s.%s",
                                  fmtId(funcInfo->dobj.namespace->dobj.name), fsig);
                free(fsig);
            } else {
                pg_log_warning("bogus value in pg_cast.castfunc or pg_cast.castmethod field");
            }
            break;
        default:
            pg_log_warning("bogus value in pg_cast.castmethod field");
    }

    // Add cast context
    if (cast->castcontext == 'a')
        appendPQExpBufferStr(defqry, " AS ASSIGNMENT");
    else if (cast->castcontext == 'i')
        appendPQExpBufferStr(defqry, " AS IMPLICIT");

    appendPQExpBufferStr(defqry, ";\n");

    // Prepare label and arguments for archiving
    appendPQExpBuffer(labelq, "CAST (%s AS %s)", sourceType, targetType);
    appendPQExpBuffer(castargs, "(%s AS %s)", sourceType, targetType);

    // Handle binary upgrade
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(defqry, &cast->dobj, "CAST", castargs->data, NULL);

    // Archive the cast
    if (cast->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, cast->dobj.catId, cast->dobj.dumpId,
                     ARCHIVE_OPTS(.tag = labelq->data,
                                  .description = "CAST",
                                  .section = SECTION_PRE_DATA,
                                  .createStmt = defqry->data,
                                  .dropStmt = delqry->data));

    // Dump cast comments
    if (cast->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "CAST", castargs->data, NULL, "",
                    cast->dobj.catId, 0, cast->dobj.dumpId);

    // Cleanup
    destroyPQExpBuffer(defqry);
    destroyPQExpBuffer(delqry);
    destroyPQExpBuffer(labelq);
    destroyPQExpBuffer(castargs);
}
```