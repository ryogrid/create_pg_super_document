# Function Simplification Task
Process each function listed in `experimental/function_call_hierarchy.txt` by passing them one by one to the
function-code-simplifier subagent.

## Instructions
1. **Read the function list**: Load all function names from `experimental/function_call_hierarchy.txt` (one function name
per line)

2. **Sequential processing**: For each function in the list:
    - Launch the function-code-simplifier subagent with the current function name
    - Wait for the subagent to report "finished" before proceeding to the next function
    - If a function fails, log the error but continue with the next function

3. **Progress reporting**:
    - Report progress after every 5 functions processed
    - Include in each report:
    - Number of functions completed vs total (e.g., "Processed 15/50 functions")
    - List of recently processed functions
    - Any errors encountered
    - Provide a summary report after every 20 functions

4. **Error handling**:
    - If a subagent fails or times out, record the failure and continue
    - Maintain a list of failed functions for final reporting
    - Do not retry failed functions automatically

5. **Final report**:
    - Total functions processed successfully
    - List of any functions that failed
    - Overall completion time

## Example Progress Output

[Progress Update - 10/50 completed]
Recently processed:
✓ XLogInsert
✓ XLogWrite✓ XLogFlush
✓ WalSndLoop
✓ WalReceiverMain

Continuing with next batch...

## Subagent Invocation

For each function, invoke the subagent with:
Task: function-code-simplifier
Input: {function_name}

Wait for response "finished" before proceeding.

Begin processing functions from the list now.