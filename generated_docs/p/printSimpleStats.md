printSimpleStats

## Overview
Prints formatted statistical summary including average and standard deviation for a set of simple statistics.

## Definition
static void printSimpleStats(const char *prefix, SimpleStats *ss)

## Detailed Description
The printSimpleStats function formats and displays basic statistical metrics for a SimpleStats data structure. It calculates and prints the average (mean) and standard deviation of the collected data points, converting from microseconds to milliseconds for display. The function only produces output if there are data points to process (count > 0), preventing division by zero and meaningless statistics display.

## Parameters / Member Variables
- prefix: String prefix to label the statistics output (e.g., "latency", "connect")
- ss: Pointer to SimpleStats structure containing count, sum, and sum2 fields for statistical calculations

## Dependencies
- Functions called/Symbols referenced:
  - sqrt - Mathematical square root function for standard deviation calculation
  - printf - Standard output formatting function
  - SimpleStats - Structure type containing statistical data
- Called from (representative examples):
  - [printResults](printResults.md) - Main results printing function that uses this for displaying latency and lag statistics

## Notes and Other Information
- Only prints statistics when count > 0 to avoid division by zero errors
- Converts time values from microseconds to milliseconds by multiplying by 0.001
- Calculates standard deviation using the formula: sqrt(sum2/count - (sum/count)^2)
- Part of pgbench results reporting system for displaying performance metrics
- Provides consistent formatting across different types of statistics (latency, lag, etc.)
- Used for final summary statistics display rather than real-time progress reporting