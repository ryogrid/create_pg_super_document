# compute_return_type

## Location
[src/backend/commands/functioncmds.c:88-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L88-L182)

## Overview
Examines the RETURNS clause of a CREATE FUNCTION statement and determines the return type information, including whether it returns a set, while handling shell type creation for new types.

## Definition

```c
structs
 * languageOid: OID of function language (InvalidOid if it's CREATE AGGREGATE)
 * objtype: identifies type of object being created
 *
 * Results are stored into output parameters.  parameterTypes must always
 * be created, but the other arrays/lists can be NULL pointers if not needed.
 * variadicArgType is set to the variadic array type if there's a VARIADIC
 * parameter (there can be only one);
```
## Detailed Description
This function is more complex than a typical typename lookup because it allows shell types to be used or even created if the specified return type doesn't exist yet. This capability is essential for defining I/O procedures for new types. However, SQL functions cannot use shell types, so the function enforces this restriction by raising an error for SQL language functions.

The function first attempts to look up the specified return type. If found but it's only a shell type (not fully defined), it issues appropriate warnings or errors depending on the function language. If the type doesn't exist at all, the function can create a shell type definition, but only for C-coded functions (INTERNAL or C language), as only these can serve as I/O functions.

## Parameters / Member Variables
- : TypeName structure specifying the desired return type from the CREATE FUNCTION statement
- : OID of the programming language for the function being created
- : Output parameter to store the resolved return type OID
- : Output parameter to indicate whether the function returns a set (based on returnType->setof)

## Dependencies
- Functions called/Symbols referenced:
  - [LookupTypeName](../L/LookupTypeName.md): Searches for an existing type definition
  - [TypeNameToString](../T/TypeNameToString.md): Converts TypeName to string representation
  - [typeTypeId](../t/typeTypeId.md): Extracts the OID from a type tuple
  - [QualifiedNameGetCreationNamespace](../Q/QualifiedNameGetCreationNamespace.md): Determines namespace for new shell type
  - [object_aclcheck](../o/object_aclcheck.md): Checks access permissions
  - [aclcheck_error](../a/aclcheck_error.md)/aclcheck_error_type: Reports permission errors
  - [TypeShellMake](../T/TypeShellMake.md): Creates a new shell type definition
- Called from (representative examples):
  - [CreateFunction](../C/CreateFunction.md): Main function creation routine

## Notes and Other Information
- Only C-coded functions (INTERNAL or C language) can return undefined shell types
- SQL functions are prohibited from using shell types to avoid runtime issues
- The function enforces namespace creation permissions when creating shell types
- Type modifiers are not allowed for shell types
- The function issues NOTICE messages when creating shell types to inform users
- Access control checks ensure the user has USAGE permission on the return type

## Simplified Source
```c
static void
compute_return_type(TypeName *returnType, Oid languageOid,
                    Oid *prorettype_p, bool *returnsSet_p)
{
    Oid rettype;
    Type typtup;

    // Try to lookup the return type
    typtup = LookupTypeName(NULL, returnType, NULL, false);

    if (typtup) {
        // Type exists - check if it's fully defined
        if (!((Form_pg_type) GETSTRUCT(typtup))->typisdefined) {
            if (languageOid == SQLlanguageId)
                ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                               errmsg("SQL function cannot return shell type %s",
                                     TypeNameToString(returnType))));
            else
                ereport(NOTICE, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                               errmsg("return type %s is only a shell",
                                     TypeNameToString(returnType))));
        }
        rettype = typeTypeId(typtup);
        ReleaseSysCache(typtup);
    }
    else {
        // Type doesn't exist - create shell type for C functions only
        char *typnam = TypeNameToString(returnType);
        Oid namespaceId;
        char *typname;
        ObjectAddress address;

        // Only C-coded functions can use undefined types
        if (languageOid != INTERNALlanguageId && languageOid != ClanguageId)
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                           errmsg("type \"%s\" does not exist", typnam)));

        // No type modifiers allowed for shell types
        if (returnType->typmods != NIL)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                           errmsg("type modifier cannot be specified for shell type \"%s\"",
                                 typnam)));

        // Create shell type with proper permissions
        ereport(NOTICE, (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("type \"%s\" is not yet defined", typnam),
                        errdetail("Creating a shell type definition.")));

        namespaceId = QualifiedNameGetCreationNamespace(returnType->names, &typname);

        // Check creation permissions
        AclResult aclresult = object_aclcheck(NamespaceRelationId, namespaceId,
                                             GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(namespaceId));

        address = TypeShellMake(typname, namespaceId, GetUserId());
        rettype = address.objectId;
    }

    // Check USAGE permission on the return type
    AclResult aclresult = object_aclcheck(TypeRelationId, rettype, GetUserId(), ACL_USAGE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error_type(aclresult, rettype);

    *prorettype_p = rettype;
    *returnsSet_p = returnType->setof;
}
```