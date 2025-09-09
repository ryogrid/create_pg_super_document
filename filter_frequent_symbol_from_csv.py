#!/usr/bin/env python3
"""
Script to count the occurrences of the 2nd element (2nd column) in a CSV file,
and display the top 40 most frequent values in descending order.
--exclude option allows exclusion of rows containing top 40 values before recounting.
When --exclude option is used, the filtered CSV data is automatically output.
"""

import csv
import argparse
from collections import Counter
from typing import List, Optional


def get_top_values_from_csv(filepath: str, top_n: int = 40) -> List[str]:
    """
    Count the 2nd elements in a CSV file and return a list of top N values
    
    Args:
        filepath: Path to the CSV file
        top_n: How many top values to retrieve
        
    Returns:
        List of top N values
    """
    second_elements = []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            
            for row_num, row in enumerate(csv_reader, 1):
                # Skip empty rows
                if not row:
                    continue
                    
                # Ensure there are at least 2 elements
                if len(row) < 2:
                    continue
                
                second_element = row[1].strip()  # Remove whitespace
                second_elements.append(second_element)
                    
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found")
        return []
    except Exception as e:
        print(f"Error: An error occurred while reading the file: {e}")
        return []
    
    # Count occurrences and get top N values
    counter = Counter(second_elements)
    top_items = counter.most_common(top_n)
    
    return [value for value, count in top_items]


def filter_csv_excluding_top_values(
    input_filepath: str,
    output_filepath: str,
    exclude_top40: bool = False
) -> int:
    """
    Output a CSV file excluding rows containing top 40 values
    
    Args:
        input_filepath: Path to the input CSV file
        output_filepath: Path to the output CSV file
        exclude_top40: Whether to exclude top 40 values
        
    Returns:
        Number of output rows
    """
    # Get list of values to exclude
    exclude_values = []
    if exclude_top40:
        exclude_values = get_top_values_from_csv(input_filepath, 40)
        if not exclude_values:
            return 0
    
    filtered_rows = []
    
    try:
        with open(input_filepath, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            
            for row_num, row in enumerate(csv_reader, 1):
                # Skip empty rows
                if not row:
                    continue
                    
                # Ensure there are at least 2 elements
                if len(row) < 2:
                    print(f"Warning: Row {row_num} does not have enough elements: {row}")
                    continue
                
                second_element = row[1].strip()  # Remove whitespace
                
                # Add only if not in exclude list
                if not exclude_top40 or second_element not in exclude_values:
                    filtered_rows.append(row)
                    
    except FileNotFoundError:
        print(f"Error: File '{input_filepath}' not found")
        return 0
    except Exception as e:
        print(f"Error: An error occurred while reading the file: {e}")
        return 0
    
    # Output filtered data
    try:
        with open(output_filepath, 'w', encoding='utf-8', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerows(filtered_rows)
            
    except Exception as e:
        print(f"Error: An error occurred while writing the file: {e}")
        return 0
    
    return len(filtered_rows)


def analyze_csv_second_column(
    filepath: str, 
    exclude_top40: bool = False,
    top_n: int = 40
) -> List[tuple]:
    """
    Count the 2nd elements in a CSV file and return them in descending order of frequency
    
    Args:
        filepath: Path to the CSV file
        exclude_top40: Whether to exclude top 40 values
        top_n: How many top values to display
        
    Returns:
        List of (value, frequency) tuples
    """
    # Get list of values to exclude
    exclude_values = []
    if exclude_top40:
        exclude_values = get_top_values_from_csv(filepath, 40)
        if not exclude_values:
            return []
    
    # List to store 2nd elements
    second_elements = []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            
            for row_num, row in enumerate(csv_reader, 1):
                # Skip empty rows
                if not row:
                    continue
                    
                # Ensure there are at least 2 elements
                if len(row) < 2:
                    print(f"Warning: Row {row_num} does not have enough elements: {row}")
                    continue
                
                second_element = row[1].strip()  # Remove whitespace
                
                # Add only if not in exclude list
                if second_element not in exclude_values:
                    second_elements.append(second_element)
                    
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found")
        return []
    except Exception as e:
        print(f"Error: An error occurred while reading the file: {e}")
        return []
    
    # Count occurrences
    counter = Counter(second_elements)
    
    # Sort by frequency and get top N
    top_items = counter.most_common(top_n)
    
    return top_items


def main():
    parser = argparse.ArgumentParser(
        description='Count occurrences of the 2nd element in a CSV file'
    )
    
    parser.add_argument(
        'filepath',
        help='Path to the CSV file'
    )
    
    parser.add_argument(
        '-e', '--exclude',
        action='store_true',
        help='Exclude rows containing top 40 values and then recount'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=str,
        help='Output file path for filtered CSV data (use with --exclude)'
    )
    
    parser.add_argument(
        '-n', '--top',
        type=int,
        default=40,
        help='Number of top entries to display (default: 40)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Display verbose information'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        print(f"File: {args.filepath}")
        if args.exclude:
            print("Exclude mode: Exclude rows containing top 40 values and then recount")
            if args.output:
                print(f"Output filtered CSV file: {args.output}")
        print(f"Display count: {args.top}")
        print("-" * 50)
    
    # Output filtered CSV file (when --exclude and --output are specified)
    if args.exclude and args.output:
        output_rows = filter_csv_excluding_top_values(
            args.filepath,
            args.output,
            True
        )
        
        if output_rows > 0:
            print(f"Filtered CSV data output to '{args.output}' ({output_rows} rows)")
            print("-" * 50)
        else:
            print("Failed to output CSV data")
            return
    elif args.exclude and not args.output:
        # When --exclude is specified but --output is not
        # Automatically generate output filename
        input_name = args.filepath.rsplit('.', 1)[0]  # Remove extension
        auto_output = f"{input_name}_filtered.csv"
        
        output_rows = filter_csv_excluding_top_values(
            args.filepath,
            auto_output,
            True
        )
        
        if output_rows > 0:
            print(f"Filtered CSV data output to '{auto_output}' ({output_rows} rows)")
            print("-" * 50)
        else:
            print("Failed to output CSV data")
            return
    
    # Execute analysis
    results = analyze_csv_second_column(
        args.filepath,
        args.exclude,
        args.top
    )
    
    if not results:
        print("No results obtained")
        return
    
    # Display results
    if args.exclude:
        print(f"Top {min(len(results), args.top)} occurrences of 2nd element (after excluding top 40):")
    else:
        print(f"Top {min(len(results), args.top)} occurrences of 2nd element:")
    print("-" * 50)
    print(f"{'Rank':<4} {'Value':<15} {'Count':<8}")
    print("-" * 50)
    
    for rank, (value, count) in enumerate(results, 1):
        print(f"{rank:<4} {value:<15} {count:<8}")
    
    if args.verbose:
        total_rows = sum(count for _, count in results)
        print("-" * 50)
        print(f"Total counted rows: {total_rows}")


if __name__ == "__main__":
    main()
