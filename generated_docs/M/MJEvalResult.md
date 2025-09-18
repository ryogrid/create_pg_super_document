# MJEvalResult

## Location
src/backend/executor/nodeMergejoin.c: 148 - 150

## Overview
MJEvalResult is an enumeration type that represents the evaluation result of tuple values during merge join operations, indicating whether a tuple is matchable, non-matchable, or signals end of join processing.

## Definition


## Detailed Description
MJEvalResult is a critical enumeration used in PostgreSQL's merge join execution to classify the state of tuple evaluation during join processing. This enum serves as the return type for the functions MJEvalOuterValues and MJEvalInnerValues, which evaluate whether outer and inner tuples can participate in the merge join operation.

The enum provides three distinct states that drive the merge join algorithm's decision-making process:
- Normal matchable tuples that can potentially participate in joins
- Non-matchable tuples that contain null values in join columns (which cannot match in inner joins)
- End-of-join conditions that signal completion of input streams

This classification system allows the merge join executor to efficiently handle different tuple states and make appropriate decisions about advancing input streams, producing output tuples, or terminating the join operation.

## Parameters / Member Variables
- : Indicates a normal tuple that is potentially matchable and can participate in join operations. This is the default state for valid tuples with non-null join column values.
- : Indicates a tuple that cannot participate in the join because it contains null values in one or more join columns. In SQL semantics, nulls don't match anything, including other nulls.
- : Indicates that the end of input has been reached, either physically (no more tuples) or effectively (due to optimization decisions). This signals the merge join algorithm to terminate processing.

## Dependencies
- Functions called/Symbols referenced: None (this is an enumeration definition)
- Called from (representative examples):
  - [MJEvalOuterValues](MJEvalOuterValues.md) (returns this type)
  - [MJEvalInnerValues](MJEvalInnerValues.md) (returns this type)
  - ExecMergeJoin (processes values of this type in switch statements)

## Notes and Other Information
- This enumeration is central to the merge join execution algorithm in PostgreSQL, appearing in multiple switch statements throughout the ExecMergeJoin function
- The enum values are used extensively in state machine logic to control the flow of the merge join algorithm
- The MJEVAL_NONMATCHABLE state is particularly important for handling SQL null semantics correctly in join operations
- Located in src/backend/executor/nodeMergejoin.c at lines 143-148
- This enum is internal to the merge join implementation and is not exposed to higher-level query processing layers