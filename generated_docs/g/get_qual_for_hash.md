# get_qual_for_hash

## Location
[src/backend/partitioning/partbounds.c:3983-4065](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L3983-L4065)

## Overview
Generates a CHECK constraint expression for a hash partition's constraint by creating a call to the built-in satisfies_hash_partition() function.

## Definition


## Detailed Description
This function constructs the partition constraint for a hash partition, which is always implemented as a call to the built-in function satisfies_hash_partition(). The function takes the parent relation and partition bound specification to create a FuncExpr that validates whether a row belongs to this specific hash partition. It builds the necessary arguments including the parent relation OID, modulus, remainder, and all partition key columns.

The generated constraint ensures that rows are properly distributed among hash partitions based on the hash values of the partition key columns. This is a critical component of PostgreSQL's hash partitioning mechanism.

## Parameters / Member Variables
- : The parent relation that is being partitioned
- : Partition bound specification containing modulus and remainder values for the hash partition

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetPartitionKey](../R/RelationGetPartitionKey.md)
  - [makeConst](../m/makeConst.md)
  - list_make3
  - list_head
  - makeVar
  - copyObject
  - [lnext](../l/lnext.md)
  - makeFuncExpr
- Called from (representative examples):
  - [get_qual_from_partbound](get_qual_from_partbound.md)

## Notes and Other Information
- The function always creates a constraint using the F_SATISFIES_HASH_PARTITION function
- Arguments include the parent relation OID, modulus, remainder, and all partition key columns
- For attribute-based partition keys, it creates Var nodes; for expression-based keys, it copies the expressions
- The resulting constraint is essential for constraint exclusion and partition pruning in hash-partitioned tables