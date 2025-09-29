# transformPartitionBound

## Location
[src/backend/parser/parse_utilcmd.c:3985-4138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L3985-L4138)

## Overview
Transforms and validates a partition bound specification according to the parent table's partitioning strategy (hash, list, or range).

## Definition
```c
PartitionBoundSpec *transformPartitionBound(ParseState *pstate, Relation parent, PartitionBoundSpec *spec)
```

## Detailed Description
This function processes partition bound specifications by validating them against the parent relation's partitioning strategy and transforming raw parse nodes into properly validated partition bounds. It handles three partitioning strategies: hash, list, and range partitioning. For hash partitioning, it validates modulus and remainder values. For list partitioning, it transforms individual list values and removes duplicates. For range partitioning, it transforms both lower and upper bound specifications. The function also handles default partitions, with special restrictions for hash partitioning.

The transformation process includes type checking, expression parsing, and ensuring that the partition bound specification matches the expected format for the given partitioning strategy. The function creates a copy of the input specification to avoid modifying the original parse tree.

## Parameters / Member Variables
- `pstate`: ParseState for error reporting and expression transformation context
- `parent`: The parent partitioned relation that defines the partitioning scheme
- `spec`: The raw partition bound specification from the parser to be transformed and validated

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetPartitionKey](../R/RelationGetPartitionKey.md)
  - [get_partition_strategy](../g/get_partition_strategy.md)
  - [get_partition_natts](../g/get_partition_natts.md)
  - [get_partition_exprs](../g/get_partition_exprs.md)
  - copyObject
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [parser_errposition](../p/parser_errposition.md)
  - [exprLocation](../e/exprLocation.md)
  - [get_attname](../g/get_attname.md)
  - [deparse_expression](../d/deparse_expression.md)
  - [deparse_context_for](../d/deparse_context_for.md)
  - [get_partition_col_typid](../g/get_partition_col_typid.md)
  - [get_partition_col_typmod](../g/get_partition_col_typmod.md)
  - [get_partition_col_collation](../g/get_partition_col_collation.md)
  - [transformPartitionBoundValue](transformPartitionBoundValue.md)
  - [transformPartitionRangeBounds](transformPartitionRangeBounds.md)
  - [equal](../e/equal.md)
  - [lappend](../l/lappend.md)
  - elog
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md) (in src/backend/commands/tablecmds.c:1108)
  - [transformPartitionCmd](transformPartitionCmd.md) (in src/backend/parser/parse_utilcmd.c:3942)

## Notes and Other Information
- [Hash](../H/Hash.md) partitioning does not support default partitions and will generate an error if attempted
- For list partitioning, duplicate values are automatically removed from the specification
- [Range](../R/Range.md) partitioning requires exact match between the number of bounds and partition key attributes
- The function preserves the original input by creating a copy using copyObject
- Validates that modulus values for hash partitioning are positive and remainder values are less than modulus
- For expression-based partitioning columns, uses deparse_expression to generate readable column names for error messages
- Returns a fully transformed PartitionBoundSpec ready for use by the execution system

## Simplified Source

```c
PartitionBoundSpec *
transformPartitionBound(ParseState *pstate, Relation parent, PartitionBoundSpec *spec)
{
    PartitionBoundSpec *result_spec;
    PartitionKey key = RelationGetPartitionKey(parent);
    char strategy = get_partition_strategy(key);
    int partnatts = get_partition_natts(key);
    List *partexprs = get_partition_exprs(key);

    // Create copy to avoid modifying input
    result_spec = copyObject(spec);

    // Handle default partition
    if (spec->is_default) {
        // Hash partitions cannot have default partition
        if (strategy == PARTITION_STRATEGY_HASH)
            ereport(ERROR, "hash-partitioned table may not have a default partition");

        result_spec->strategy = strategy;
        return result_spec;
    }

    // Handle hash partitioning
    if (strategy == PARTITION_STRATEGY_HASH) {
        // Validate strategy matches
        if (spec->strategy != PARTITION_STRATEGY_HASH)
            ereport(ERROR, "invalid bound specification for a hash partition");

        // Validate modulus and remainder values
        if (spec->modulus <= 0)
            ereport(ERROR, "modulus must be greater than zero");
        if (spec->remainder >= spec->modulus)
            ereport(ERROR, "remainder must be less than modulus");
    }
    // Handle list partitioning
    else if (strategy == PARTITION_STRATEGY_LIST) {
        ListCell *cell;

        // Validate strategy matches
        if (spec->strategy != PARTITION_STRATEGY_LIST)
            ereport(ERROR, "invalid bound specification for a list partition");

        // Get column info for value transformation
        char *colname = get_partition_column_name(key, parent, partexprs);
        Oid coltype = get_partition_col_typid(key, 0);
        int32 coltypmod = get_partition_col_typmod(key, 0);
        Oid partcollation = get_partition_col_collation(key, 0);

        // Transform and deduplicate list values
        result_spec->listdatums = NIL;
        foreach(cell, spec->listdatums) {
            Node *expr = lfirst(cell);
            Const *value = transformPartitionBoundValue(pstate, expr, colname,
                                                       coltype, coltypmod, partcollation);

            // Add value if not duplicate
            if (!list_contains_const(result_spec->listdatums, value))
                result_spec->listdatums = lappend(result_spec->listdatums, value);
        }
    }
    // Handle range partitioning
    else if (strategy == PARTITION_STRATEGY_RANGE) {
        // Validate strategy matches
        if (spec->strategy != PARTITION_STRATEGY_RANGE)
            ereport(ERROR, "invalid bound specification for a range partition");

        // Validate bound counts match partition key attributes
        if (list_length(spec->lowerdatums) != partnatts ||
            list_length(spec->upperdatums) != partnatts)
            ereport(ERROR, "bound count must match partitioning column count");

        // Transform lower and upper bounds
        result_spec->lowerdatums = transformPartitionRangeBounds(pstate, spec->lowerdatums, parent);
        result_spec->upperdatums = transformPartitionRangeBounds(pstate, spec->upperdatums, parent);
    }
    else {
        elog(ERROR, "unexpected partition strategy: %d", (int) strategy);
    }

    return result_spec;
}
```