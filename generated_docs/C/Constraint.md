# Constraint

## Location
src/include/nodes/parsenodes.h: 2728 - 2773

## Overview
Constraint is a comprehensive parse tree node structure that represents all types of table and column constraints in PostgreSQL, including CHECK, NOT NULL, UNIQUE, PRIMARY KEY, FOREIGN KEY, DEFAULT, and EXCLUSION constraints.

## Definition
```c
typedef struct Constraint
{
    NodeTag         type;
    ConstrType      contype;            /* constraint type (see ConstrType enum) */
    char           *conname;            /* Constraint name, or NULL if unnamed */
    bool            deferrable;         /* DEFERRABLE? */
    bool            initdeferred;       /* INITIALLY DEFERRED? */
    bool            skip_validation;    /* skip validation of existing rows? */
    bool            initially_valid;    /* mark the new constraint as valid? */
    bool            is_no_inherit;      /* is constraint non-inheritable? */
    Node           *raw_expr;           /* CHECK or DEFAULT expression, as untransformed parse tree */
    char           *cooked_expr;        /* CHECK or DEFAULT expression, as nodeToString representation */
    char            generated_when;     /* ALWAYS or BY DEFAULT */
    int             inhcount;           /* initial inheritance count to apply, for "raw" NOT NULL constraints */
    bool            nulls_not_distinct; /* null treatment for UNIQUE constraints */
    List           *keys;               /* String nodes naming referenced key column(s); for UNIQUE/PK/NOT NULL */
    List           *including;          /* String nodes naming referenced nonkey column(s); for UNIQUE/PK */
    List           *exclusions;         /* list of (IndexElem, operator name) pairs; for exclusion constraints */
    List           *options;            /* options from WITH clause */
    char           *indexname;          /* existing index to use; otherwise NULL */
    char           *indexspace;         /* index tablespace; NULL for default */
    bool            reset_default_tblspc; /* reset default_tablespace prior to creating the index */
    char           *access_method;      /* index access method; NULL for default */
    Node           *where_clause;       /* partial index predicate */
    
    /* Fields used for FOREIGN KEY constraints: */
    RangeVar       *pktable;            /* Primary key table */
    List           *fk_attrs;           /* Attributes of foreign key */
    List           *pk_attrs;           /* Corresponding attrs in PK table */
    char            fk_matchtype;       /* FULL, PARTIAL, SIMPLE */
    char            fk_upd_action;      /* ON UPDATE action */
    char            fk_del_action;      /* ON DELETE action */
    List           *fk_del_set_cols;    /* ON DELETE SET NULL/DEFAULT (col1, col2) */
    List           *old_conpfeqop;      /* pg_constraint.conpfeqop of my former self */
    Oid             old_pktable_oid;    /* pg_constraint.confrelid of my former self */
    
    ParseLoc        location;           /* token location, or -1 if unknown */
} Constraint;
```

## Detailed Description
Constraint is a unified structure that represents all constraint types in PostgreSQL. It serves as a parse tree node for constraint definitions that appear in CREATE TABLE, ALTER TABLE, and CREATE DOMAIN statements. The structure is designed to handle the diverse requirements of different constraint types within a single node type.

The constraint supports both "raw" and "cooked" forms of expressions. Raw expressions are untransformed parse trees from initial parsing, while cooked expressions are nodeToString representations of executable expression trees from inherited relations. A constraint node should never contain both forms simultaneously.

For constraint validation, the structure provides flags to control whether existing table data should be validated against new constraints and whether the constraint should initially be marked as valid in the catalog.

The structure includes specialized fields for different constraint types: expression fields for CHECK and DEFAULT constraints, key lists for UNIQUE and PRIMARY KEY constraints, and comprehensive foreign key relationship fields for FOREIGN KEY constraints.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a Constraint node type
- `contype`: ConstrType enum value specifying the constraint type (CONSTR_CHECK, CONSTR_PRIMARY, etc.)
- `conname`: Optional constraint name; NULL for unnamed constraints
- `deferrable`: Boolean flag for DEFERRABLE constraint attribute
- `initdeferred`: Boolean flag for INITIALLY DEFERRED constraint attribute  
- `skip_validation`: Skip validation of existing rows when adding constraint
- `initially_valid`: Mark the constraint as valid in catalog (usually inverse of skip_validation)
- `is_no_inherit`: Constraint is non-inheritable (NO INHERIT attribute)
- `raw_expr`: Untransformed parse tree for CHECK or DEFAULT expressions
- `cooked_expr`: String representation of executable expression tree
- `generated_when`: Generation timing for generated columns (ALWAYS or BY DEFAULT)
- `inhcount`: Initial inheritance count for "raw" NOT NULL constraints
- `nulls_not_distinct`: NULL treatment for UNIQUE constraints (NULLS NOT DISTINCT)
- `keys`: List of column names for UNIQUE/PRIMARY KEY/NOT NULL constraints
- `including`: List of included non-key columns for UNIQUE/PRIMARY KEY constraints
- `exclusions`: List of (IndexElem, operator) pairs for exclusion constraints
- `options`: Storage options from WITH clause
- `indexname`: Name of existing index to use for constraint
- `indexspace`: Tablespace for constraint index
- `reset_default_tblspc`: Reset default_tablespace before creating index
- `access_method`: Index access method for constraint index
- `where_clause`: Partial index predicate for constraint index
- `pktable`: RangeVar specifying referenced table for foreign key constraints
- `fk_attrs`: List of foreign key column names
- `pk_attrs`: List of referenced primary key column names
- `fk_matchtype`: Foreign key match type (FULL, PARTIAL, SIMPLE)
- `fk_upd_action`: ON UPDATE action for foreign key
- `fk_del_action`: ON DELETE action for foreign key
- `fk_del_set_cols`: Column list for ON DELETE SET NULL/DEFAULT
- `old_conpfeqop`: Previous constraint equality operators (for ALTER operations)
- `old_pktable_oid`: Previous referenced table OID (for ALTER operations)
- `location`: Token location in source text, or -1 if unknown

## Dependencies
- Functions called/Symbols referenced:
  - ConstrType (constraint type enumeration)
  - RangeVar (for foreign key table references)
  - ParseLoc (for source location tracking)

- Called from (representative examples):
  - AddRelationNewConstraints (src/backend/catalog/heap.c:2409)
  - ATExecAddConstraint (src/backend/commands/tablecmds.c:9356)
  - DefineDomain (src/backend/commands/typecmds.c:867)
  - transformCreateStmt (src/backend/parser/parse_utilcmd.c:281)
  - transformTableConstraint (src/backend/parser/parse_utilcmd.c:903)

## Notes and Other Information
- Column defaults are treated as constraints semantically, even though this is conceptually unusual
- The structure undergoes transformation during parse analysis to separate constraint attributes into the appropriate constraint nodes
- Foreign key action and match type codes are stored directly in pg_constraint catalog entries
- Skip_validation and initially_valid flags provide fine-grained control over constraint validation during DDL operations
- The structure supports both table-level and column-level constraints through the same interface
- Constraint attributes (DEFERRABLE, etc.) are initially parsed as separate constraint nodes and later merged during parse_utilcmd.c processing
- Expression constraints can exist in either raw or cooked form, but never both simultaneously in the same node