# get_other_operator

## Location
[src/backend/catalog/pg_operator.c:622-683](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_operator.c#L622-L683)

## Overview
Looks up or creates a related operator (such as a commutator or negator) referenced during operator creation, handling cases where the operator exists, is the same as the operator being defined, or needs to be created as a shell.

## Definition
```c
static Oid get_other_operator(List *otherOp, Oid otherLeftTypeId, Oid otherRightTypeId,
                              const char *operatorName, Oid operatorNamespace,
                              Oid leftTypeId, Oid rightTypeId)
```

## Detailed Description
This static helper function handles the complex logic of resolving references to related operators during operator creation. It follows a three-step resolution process:

1. **Existing Operator Lookup**: First attempts to find the referenced operator in the system catalogs using OperatorLookup.

2. **Self-Reference Detection**: If not found, checks whether the referenced operator would be identical to the operator currently being defined (same name, namespace, and argument types). This handles cases where an operator is its own commutator or negator.

3. **Shell Operator Creation**: If the operator doesn't exist and isn't a self-reference, creates a "shell" operator entry in the catalogs. Shell operators are placeholder entries that can be filled in later when the actual operator is defined.

The function also performs permission checks to ensure the user has CREATE privileges in the target namespace before creating shell operators.

## Parameters / Member Variables
- `otherOp`: List representing the qualified name of the related operator to look up
- `otherLeftTypeId`: OID of the left argument type for the related operator
- `otherRightTypeId`: OID of the right argument type for the related operator
- `operatorName`: Name of the operator currently being defined
- `operatorNamespace`: Namespace OID of the operator currently being defined
- `leftTypeId`: Left argument type OID of the operator currently being defined
- `rightTypeId`: Right argument type OID of the operator currently being defined

## Dependencies
- Functions called/Symbols referenced:
  - [OperatorLookup](../O/OperatorLookup.md) (looks up existing operators)
  - [QualifiedNameGetCreationNamespace](../Q/QualifiedNameGetCreationNamespace.md) (resolves namespace from qualified name)
  - [object_aclcheck](../o/object_aclcheck.md) (checks permissions)
  - [aclcheck_error](../a/aclcheck_error.md) (reports permission errors)
  - [get_namespace_name](get_namespace_name.md) (gets namespace name for error messages)
  - [OperatorShellMake](../O/OperatorShellMake.md) (creates shell operator entries)
- Called from (representative examples):
  - [OperatorCreate](../O/OperatorCreate.md) (at src/backend/catalog/pg_operator.c:400)
  - [OperatorCreate](../O/OperatorCreate.md) (at src/backend/catalog/pg_operator.c:425)

## Notes and Other Information
- Returns InvalidOid for self-references, which the caller must handle appropriately
- Shell operators allow forward references in operator definitions, enabling circular dependencies like commutator pairs
- Permission checks ensure users can only create operators in namespaces where they have CREATE privileges
- Used specifically for resolving commutator and negator operator references during CREATE OPERATOR

## Simplified Source

```c
static Oid
get_other_operator(List *otherOp, Oid otherLeftTypeId, Oid otherRightTypeId,
                   const char *operatorName, Oid operatorNamespace,
                   Oid leftTypeId, Oid rightTypeId)
{
    Oid other_oid;
    bool otherDefined;

    // First, try to find existing operator
    other_oid = OperatorLookup(otherOp, otherLeftTypeId, otherRightTypeId, &otherDefined);

    if (OidIsValid(other_oid)) {
        // Operator already exists in catalogs
        return other_oid;
    }

    // Resolve namespace and name for the other operator
    char *otherName;
    Oid otherNamespace = QualifiedNameGetCreationNamespace(otherOp, &otherName);

    // Check if this is a self-reference (operator refers to itself)
    if (strcmp(otherName, operatorName) == 0 &&
        otherNamespace == operatorNamespace &&
        otherLeftTypeId == leftTypeId &&
        otherRightTypeId == rightTypeId) {
        // Self-linkage - caller must handle this case
        return InvalidOid;
    }

    // Different operator that doesn't exist - check permissions and create shell
    AclResult aclresult = object_aclcheck(NamespaceRelationId, otherNamespace,
                                         GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(otherNamespace));

    // Create shell operator for future completion
    other_oid = OperatorShellMake(otherName, otherNamespace,
                                 otherLeftTypeId, otherRightTypeId);
    return other_oid;
}
```