# DefineDomain

## Location
[src/backend/commands/typecmds.c:697-1146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L697-L1146)

## Overview
DefineDomain creates a new domain type, which is a specialized type that inherits properties from a base type but can have additional constraints, default values, and NOT NULL specifications applied.

## Definition

```c
enum or a range type.  Domains over pseudotypes would create a
	 * security hole.  (It would be shorter to code this to just check for
	 * pseudotypes;
```
## Detailed Description
DefineDomain implements PostgreSQL's CREATE DOMAIN command by creating a new domain type that acts as a constrained version of an existing base type. Domains allow users to define commonly-used data types with specific constraints that are automatically applied wherever the domain is used.

The function performs several key operations:
1. **Base Type Validation**: Ensures the base type is valid and supports domain creation (base types, composite types, other domains, enums, ranges, and multiranges are supported, but pseudotypes are not)
2. **Property Inheritance**: Inherits most properties from the base type including I/O functions, alignment, storage, and physical characteristics
3. **Constraint Processing**: Handles DEFAULT, NOT NULL, NULL, and CHECK constraints specific to the domain
4. **Array Type Creation**: Automatically creates a corresponding array type for the domain
5. **Collation Management**: Handles collation inheritance and explicit collation specification

The function uses specialized domain I/O functions (domain_in, domain_recv) that perform constraint checking during input operations, while output functions are inherited from the base type.

## Parameters / Member Variables
- : CreateDomainStmt structure containing all domain definition information
  - localdomain: Qualified name list for the new domain
  - : TypeName structure identifying the base type
  - : List of constraints to apply to the domain
  - : Optional collation specification

### Key Constraint Types Supported:
- : Provides default value for the domain
- : Makes the domain non-nullable
- : Explicitly allows NULL (overrides base type)
- : Adds check constraints for value validation

## Dependencies
- Functions called/Symbols referenced:
  - [TypeCreate](../T/TypeCreate.md): Creates the actual domain and array type entries
  - [typenameType](../t/typenameType.md): Resolves the base type name to a type tuple
  - [moveArrayTypeName](../m/moveArrayTypeName.md): Handles array type name conflicts
  - [makeArrayTypeName](../m/makeArrayTypeName.md): Generates array type names
  - [AssignTypeArrayOid](../A/AssignTypeArrayOid.md): Allocates OID for the array type
  - [QualifiedNameGetCreationNamespace](../Q/QualifiedNameGetCreationNamespace.md): Parses qualified names
  - [cookDefault](../c/cookDefault.md): Processes default value expressions
  - [domainAddCheckConstraint](../d/domainAddCheckConstraint.md): Adds domain-specific check constraints
  - [domainAddNotNullConstraint](../d/domainAddNotNullConstraint.md): Adds domain-specific NOT NULL constraints
  - [get_collation_oid](../g/get_collation_oid.md): Resolves collation names

- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main DDL command processing

## Notes and Other Information
- Domains inherit most physical properties from their base type but can override logical constraints
- Domain I/O functions (F_DOMAIN_IN, F_DOMAIN_RECV) perform constraint validation during input
- Domains cannot have type modifiers (typmodin/typmodout are always InvalidOid)
- Domains don't support subscripting directly (the parser reduces to base type before subscripting)
- The function automatically creates array types for domains, following the same pattern as base types
- Check constraints are processed after domain creation since they need the domain's OID
- Supports inheritance of default values from base type, with ability to override
- Collation must be compatible with the base type's collation capabilities
- Domain array types use standard array I/O functions but inherit the domain's constraints through the element type

## Simplified Source

```c
ObjectAddress
DefineDomain(CreateDomainStmt *stmt)
{
    char *domainName;
    Oid domainNamespace;
    Oid basetypeoid;
    Oid domainArrayOid;

    // Type properties inherited from base type
    int16 internalLength;
    bool byValue;
    char alignment;
    char storage;
    char category;
    char delimiter;

    // Domain-specific properties
    char *defaultValue = NULL;
    char *defaultValueBin = NULL;
    bool typNotNull = false;
    bool saw_default = false;
    bool nullDefined = false;
    Oid domaincoll;

    ObjectAddress address;

    // Extract domain name and validate namespace permissions
    domainNamespace = QualifiedNameGetCreationNamespace(stmt->domainname, &domainName);
    check_namespace_permissions(domainNamespace);

    // Check for naming conflicts with existing types
    Oid old_type_oid = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
                                       CStringGetDatum(domainName),
                                       ObjectIdGetDatum(domainNamespace));
    if (OidIsValid(old_type_oid)) {
        if (!moveArrayTypeName(old_type_oid, domainName, domainNamespace))
            ereport(ERROR, "type \"%s\" already exists", domainName);
    }

    // Look up and validate the base type
    HeapTuple typeTup = typenameType(NULL, stmt->typeName, &basetypeMod);
    Form_pg_type baseType = (Form_pg_type) GETSTRUCT(typeTup);
    basetypeoid = baseType->oid;

    // Validate base type is suitable for domains
    char typtype = baseType->typtype;
    if (typtype != TYPTYPE_BASE && typtype != TYPTYPE_COMPOSITE &&
        typtype != TYPTYPE_DOMAIN && typtype != TYPTYPE_ENUM &&
        typtype != TYPTYPE_RANGE && typtype != TYPTYPE_MULTIRANGE)
        ereport(ERROR, "\"%s\" is not a valid base type for a domain",
                TypeNameToString(stmt->typeName));

    check_type_permissions(basetypeoid);

    // Inherit properties from base type
    byValue = baseType->typbyval;
    alignment = baseType->typalign;
    storage = baseType->typstorage;
    internalLength = baseType->typlen;
    category = baseType->typcategory;
    delimiter = baseType->typdelim;

    // Set up domain-specific I/O functions
    Oid inputProcedure = F_DOMAIN_IN;
    Oid outputProcedure = baseType->typoutput;
    Oid receiveProcedure = F_DOMAIN_RECV;
    Oid sendProcedure = baseType->typsend;
    Oid analyzeProcedure = baseType->typanalyze;

    // Handle collation inheritance and specification
    Oid baseColl = baseType->typcollation;
    if (stmt->collClause)
        domaincoll = get_collation_oid(stmt->collClause->collname, false);
    else
        domaincoll = baseColl;

    // Validate collation compatibility
    if (OidIsValid(domaincoll) && !OidIsValid(baseColl))
        ereport(ERROR, "collations are not supported by type %s",
                format_type_be(basetypeoid));

    // Inherit default values from base type
    extract_base_type_defaults(typeTup, &defaultValue, &defaultValueBin);

    // Process domain constraints
    foreach(listptr, stmt->constraints) {
        Constraint *constr = lfirst(listptr);

        switch (constr->contype) {
            case CONSTR_DEFAULT:
                if (saw_default)
                    ereport(ERROR, "multiple default expressions");
                saw_default = true;

                if (constr->raw_expr) {
                    // Parse and validate default expression
                    ParseState *pstate = make_parsestate(NULL);
                    Node *defaultExpr = cookDefault(pstate, constr->raw_expr,
                                                   basetypeoid, basetypeMod,
                                                   domainName, 0);

                    if (defaultExpr && !IsA(defaultExpr, Const) ||
                        !((Const *) defaultExpr)->constisnull) {
                        defaultValue = deparse_expression(defaultExpr, NIL, false, false);
                        defaultValueBin = nodeToString(defaultExpr);
                    } else {
                        defaultValue = NULL;
                        defaultValueBin = NULL;
                    }
                }
                break;

            case CONSTR_NOTNULL:
                if (nullDefined && !typNotNull)
                    ereport(ERROR, "conflicting NULL/NOT NULL constraints");
                typNotNull = true;
                nullDefined = true;
                break;

            case CONSTR_NULL:
                if (nullDefined && typNotNull)
                    ereport(ERROR, "conflicting NULL/NOT NULL constraints");
                typNotNull = false;
                nullDefined = true;
                break;

            case CONSTR_CHECK:
                // Check constraints processed after domain creation
                if (constr->is_no_inherit)
                    ereport(ERROR, "check constraints for domains cannot be marked NO INHERIT");
                break;

            // Reject unsupported constraint types
            case CONSTR_UNIQUE:
            case CONSTR_PRIMARY:
            case CONSTR_EXCLUSION:
            case CONSTR_FOREIGN:
                ereport(ERROR, "constraint type not supported for domains");
                break;
        }
    }

    // Allocate OID for corresponding array type
    domainArrayOid = AssignTypeArrayOid();

    // Create the domain type
    address = TypeCreate(InvalidOid, domainName, domainNamespace,
                        InvalidOid, 0, GetUserId(),
                        internalLength, TYPTYPE_DOMAIN, category,
                        false, delimiter,
                        inputProcedure, outputProcedure,
                        receiveProcedure, sendProcedure,
                        InvalidOid, InvalidOid, analyzeProcedure, InvalidOid,
                        InvalidOid, false, domainArrayOid,
                        basetypeoid, defaultValue, defaultValueBin,
                        byValue, alignment, storage,
                        basetypeMod, typNDims, typNotNull, domaincoll);

    // Create the corresponding array type
    char *domainArrayName = makeArrayTypeName(domainName, domainNamespace);
    create_domain_array_type(domainArrayOid, domainArrayName, domainNamespace,
                            address.objectId, delimiter, domaincoll);
    pfree(domainArrayName);

    // Process constraints that need the domain OID
    foreach(listptr, stmt->constraints) {
        Constraint *constr = lfirst(listptr);

        switch (constr->contype) {
            case CONSTR_CHECK:
                domainAddCheckConstraint(address.objectId, domainNamespace,
                                       basetypeoid, basetypeMod,
                                       constr, domainName, NULL);
                break;

            case CONSTR_NOTNULL:
                domainAddNotNullConstraint(address.objectId, domainNamespace,
                                         basetypeoid, basetypeMod,
                                         constr, domainName, NULL);
                break;
        }

        CommandCounterIncrement();
    }

    ReleaseSysCache(typeTup);
    return address;
}
```