# get_qual_for_hash

## Location
[src/backend/partitioning/partbounds.c:3983-4065](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L3983-L4065)

## Overview
Generates a CHECK constraint expression for a hash partition's constraint by creating a call to the built-in satisfies_hash_partition() function.

## Definition

```c
static List *
get_qual_for_hash(Relation parent, PartitionBoundSpec *spec)
```
## Detailed Description
This function constructs the partition constraint for a hash partition, which is always implemented as a call to the built-in function satisfies_hash_partition(). The function takes the parent relation and partition bound specification to create a FuncExpr that validates whether a row belongs to this specific hash partition. It builds the necessary arguments including the parent relation OID, modulus, remainder, and all partition key columns.

The generated constraint ensures that rows are properly distributed among hash partitions based on the hash values of the partition key columns. This is a critical component of PostgreSQL's hash partitioning mechanism.

## Parameters / Member Variables
- `parent`: The parent relation that is being partitioned
- `*spec`: Partition bound specification containing modulus and remainder values for the hash partition
## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetPartitionKey](../R/RelationGetPartitionKey.md)
  - [makeConst](../m/makeConst.md)
  - list_make3
  - [list_head](../l/list_head.md)
  - [makeVar](../m/makeVar.md)
  - copyObject
  - [lnext](../l/lnext.md)
  - [makeFuncExpr](../m/makeFuncExpr.md)
- Called from (representative examples):
  - [get_qual_from_partbound](get_qual_from_partbound.md)

## Notes and Other Information
- The function always creates a constraint using the F_SATISFIES_HASH_PARTITION function
- Arguments include the parent relation OID, modulus, remainder, and all partition key columns
- For attribute-based partition keys, it creates Var nodes; for expression-based keys, it copies the expressions
- The resulting constraint is essential for constraint exclusion and partition pruning in hash-partitioned tables

## Simplified Source

```c
static List *
get_qual_for_hash(Relation parent, PartitionBoundSpec *spec)
{
    PartitionKey key = RelationGetPartitionKey(parent);
    FuncExpr *fexpr;
    Node *relidConst;
    Node *modulusConst;
    Node *remainderConst;
    List *args;
    ListCell *partexprs_item;
    int i;

    // Create fixed arguments for satisfies_hash_partition()
    relidConst = (Node *) makeConst(OIDOID,
                                   -1,
                                   InvalidOid,
                                   sizeof(Oid),
                                   ObjectIdGetDatum(RelationGetRelid(parent)),
                                   false,
                                   true);

    modulusConst = (Node *) makeConst(INT4OID,
                                     -1,
                                     InvalidOid,
                                     sizeof(int32),
                                     Int32GetDatum(spec->modulus),
                                     false,
                                     true);

    remainderConst = (Node *) makeConst(INT4OID,
                                       -1,
                                       InvalidOid,
                                       sizeof(int32),
                                       Int32GetDatum(spec->remainder),
                                       false,
                                       true);

    args = list_make3(relidConst, modulusConst, remainderConst);
    partexprs_item = list_head(key->partexprs);

    // Add an argument for each partition key column
    for (i = 0; i < key->partnatts; i++)
    {
        Node *keyCol;

        if (key->partattrs[i] != 0)
        {
            // Create Var node for attribute-based partition key
            keyCol = (Node *) makeVar(1,
                                     key->partattrs[i],
                                     key->parttypid[i],
                                     key->parttypmod[i],
                                     key->parttypcoll[i],
                                     0);
        }
        else
        {
            // Copy expression for expression-based partition key
            keyCol = (Node *) copyObject(lfirst(partexprs_item));
            partexprs_item = lnext(key->partexprs, partexprs_item);
        }

        args = lappend(args, keyCol);
    }

    // Create function expression for satisfies_hash_partition()
    fexpr = makeFuncExpr(F_SATISFIES_HASH_PARTITION,
                        BOOLOID,
                        args,
                        InvalidOid,
                        InvalidOid,
                        COERCE_EXPLICIT_CALL);

    return list_make1(fexpr);
}
```