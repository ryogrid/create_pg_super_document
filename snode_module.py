#!/usr/bin/env python3
"""
SNode module - Provides classes for handling symbol information
"""

import os
import sys
import duckdb
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from functools import lru_cache

 # Database settings
DB_FILE = "global_symbols.db"
SYMBOL_TABLE = "symbol_definitions"
REFERENCE_TABLE = "symbol_reference"


class DatabaseConnection:
    """
    Singleton class to manage database connections
    """
    _instance = None
    _connection = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_connection(self):
        """Get database connection"""
        if self._connection is None:
            if not Path(DB_FILE).exists():
                raise FileNotFoundError(f"Database file '{DB_FILE}' not found.")
            self._connection = duckdb.connect(DB_FILE, read_only=False)
        return self._connection
    
    def close(self):
        """Close database connection"""
        if self._connection:
            self._connection.close()
            self._connection = None


class SNode:
    """
    Node class representing symbol information
    """
    
    # Database connection（class variable）
    _db = DatabaseConnection()
    
    def __init__(self, symbol_name: str):
        """
        Create an SNode object from a symbol name
        
        Args:
            symbol_name: Symbol name
        """
        self.symbol_name = symbol_name
        self._contents = None  # for lazy loading of source code

        # Retrieve symbol information from the database (using the record with the smallest ID)
        conn = self._db.get_connection()
        result = conn.execute(f"""
            SELECT id, file_path, line_num_start, line_num_end, symbol_type
            FROM {SYMBOL_TABLE}
            WHERE symbol_name = ?
            ORDER BY id
            LIMIT 1
        """, (symbol_name,)).fetchone()
        
        if not result:
            raise ValueError(f"Symbol '{symbol_name}' not found in database")
        
        # フィールドに格納
        self.id = result[0]
        self.file_path = result[1]
        self.line_num_start = result[2]
        self.line_num_end = result[3]
        self.symbol_type = result[4]

    @classmethod
    def from_id(cls, record_id: int) -> 'SNode':
        """
        Factory function to create an SNode object from a record ID.
        
        Args:
            record_id: ID of the symbol_definitions table
        
        Returns:
            SNode: Created SNode object
        """
        conn = cls._db.get_connection()
        result = conn.execute(f"""
            SELECT symbol_name, file_path, line_num_start, line_num_end
            FROM {SYMBOL_TABLE}
            WHERE id = ?
        """, (record_id,)).fetchone()
        
        if not result:
            raise ValueError(f"Record with ID {record_id} not found in database")
        
    # Create SNode object (bypassing constructor and setting directly)
        node = object.__new__(cls)
        node.id = record_id
        node.symbol_name = result[0]
        node.file_path = result[1]
        node.line_num_start = result[2]
        node.line_num_end = result[3]
        node._contents = None
        
        return node
    
    def get_source_code(self) -> str:
        """
        Returns the source code of this symbol as a string.
        Includes comments and return type, but excludes comments for the next symbol.
        
        Returns:
            str: Source code (with header information)
        """
    # Return if already loaded
        if self._contents is not None:
            return self._contents
        
    # Check if file exists
        if not Path(self.file_path).exists():
            raise FileNotFoundError(f"Source file '{self.file_path}' not found")
        
    # Read the relevant lines from the file
        try:
            with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            
            # Line numbers are 1-based, array indices are 0-based
            original_start_idx = self.line_num_start - 1
            original_end_idx = self.line_num_end if self.line_num_end > 0 else len(lines)
            
            # Adjust actual start and end positions
            actual_start_idx = self._find_actual_start(lines, original_start_idx)
            actual_end_idx = self._find_actual_end(lines, original_start_idx, original_end_idx)
            
            # Adjusted line numbers (1-based)
            actual_start_line = actual_start_idx + 1
            actual_end_line = actual_end_idx  # actual_end_idx is exclusive, so the last line is actual_end_idx
            
            # Create header information
            header = f"Source: {self.file_path}:{actual_start_line}-{actual_end_line}\n"
            
            # Join the relevant lines
            source_code = ''.join(lines[actual_start_idx:actual_end_idx])
            
            # Combine header and source code
            self._contents = header + source_code
            
        except Exception as e:
            raise RuntimeError(f"Error reading source file: {e}")
        
        return self._contents
    
    def _find_actual_start(self, lines: List[str], original_start_idx: int) -> int:
        """
        Find the actual start position of the symbol definition (including comments and return type).
        
        Args:
            lines: All lines of the file
            original_start_idx: Original start index (0-based)
        
        Returns:
            int: Actual start index
        """
        if original_start_idx == 0:
            return 0
        
    # Search backward from the current position
        idx = original_start_idx - 1
        actual_start = original_start_idx
        in_comment = False
        comment_start = -1
        
        while idx >= 0:
            line = lines[idx].rstrip()
            
            # Detect end of block comment
            if '*/' in line and not in_comment:
                in_comment = True
                comment_start = idx
            
            # Detect start of block comment
            if '/*' in line and in_comment:
                actual_start = idx
                in_comment = False
                idx -= 1
                continue
            
            # Skip if inside a comment
            if in_comment:
                idx -= 1
                continue
            
            # Blank line or line with only whitespace
            if not line or line.isspace():
                idx -= 1
                continue
            
            # Single-line comment
            if line.strip().startswith('//'):
                actual_start = idx
                idx -= 1
                continue
            
            # Preprocessor directive
            if line.strip().startswith('#'):
                # Do not include #define or #ifdef, as it may be another symbol definition
                break
            
            # Return type of function or modifiers like static/extern
            # Lines ending with semicolon or opening brace may be another definition
            if line.endswith(';') or line.endswith('{'):
                break
            
            # typedef, struct/enum/union keywords
            keywords = ['typedef', 'struct', 'enum', 'union', 'static', 'extern', 
                       'const', 'volatile', 'inline', 'register']
            line_lower = line.lower()
            if any(keyword in line_lower for keyword in keywords):
                # May continue to the next line, so include it
                actual_start = idx
                idx -= 1
                continue
            
            # Other cases (such as variable types)
            # Lines starting with an alphabet may be a return type
            if line and line[0].isalpha():
                actual_start = idx
                idx -= 1
                continue
            
            # In other cases, stop searching
            break
        
        return actual_start
    
    def _find_actual_end(self, lines: List[str], start_idx: int, original_end_idx: int) -> int:
        """
        Find the actual end position of the symbol definition (excluding comments for the next symbol).
        
        Args:
            lines: All lines of the file
            start_idx: Start index (0-based)
            original_end_idx: Original end index (0-based, exclusive)
        
        Returns:
            int: Actual end index
        """
        # Default is the original end position
        actual_end = original_end_idx

        # Determine the type of symbol
        if start_idx < len(lines):
            first_line = lines[start_idx].strip()
            
            # In case of macro definition
            if first_line.startswith('#define'):
                # Track continuation lines (ending with \)
                idx = start_idx
                while idx < original_end_idx and idx < len(lines):
                    line = lines[idx].rstrip()
                    if not line.endswith('\\'):
                        return min(idx + 1, original_end_idx)
                    idx += 1
                return original_end_idx
        
    # For functions or structs, track matching braces
        brace_count = 0
        found_first_brace = False
        idx = start_idx
        
        while idx < original_end_idx and idx < len(lines):
            line = lines[idx]
            
            # Count braces excluding string literals and comments
            in_string = False
            in_char = False
            in_line_comment = False
            in_block_comment = False
            prev_char = ''
            
            i = 0
            while i < len(line):
                char = line[i]
                
                # String literal
                if char == '"' and prev_char != '\\' and not in_char and not in_line_comment and not in_block_comment:
                    in_string = not in_string
                # Character literal
                elif char == "'" and prev_char != '\\' and not in_string and not in_line_comment and not in_block_comment:
                    in_char = not in_char
                # Line comment
                elif i < len(line) - 1 and line[i:i+2] == '//' and not in_string and not in_char and not in_block_comment:
                    in_line_comment = True
                    i += 1
                # Block comment start
                elif i < len(line) - 1 and line[i:i+2] == '/*' and not in_string and not in_char and not in_line_comment:
                    in_block_comment = True
                    i += 1
                # Block comment end
                elif i < len(line) - 1 and line[i:i+2] == '*/' and in_block_comment:
                    in_block_comment = False
                    i += 1
                # Count braces
                elif not in_string and not in_char and not in_line_comment and not in_block_comment:
                    if char == '{':
                        brace_count += 1
                        found_first_brace = True
                    elif char == '}':
                        brace_count -= 1
                        if found_first_brace and brace_count == 0:
                            # For struct, include up to semicolon after }
                            remaining = line[i+1:].strip()
                            if remaining.startswith(';'):
                                return min(idx + 1, original_end_idx)
                            # For typedef struct, include up to type name definition
                            elif remaining and not remaining.startswith('/'):
                                # Check the next line as well
                                if idx + 1 < original_end_idx:
                                    next_line = lines[idx + 1].strip()
                                    if next_line.startswith(';'):
                                        return min(idx + 2, original_end_idx)
                                return min(idx + 1, original_end_idx)
                            else:
                                return min(idx + 1, original_end_idx)
                
                prev_char = char if char != '\\' else prev_char
                i += 1
            
            idx += 1
        
    # If matching braces are not found, exclude the next comment
    # Search backward from the end position for the last non-blank, non-comment line
        idx = original_end_idx - 1
        while idx > start_idx:
            line = lines[idx].strip()
            
            # If a non-blank, non-comment line is found, include up to there
            if line and not line.startswith('/*') and not line.startswith('*') and not line.startswith('//'):
                # Check if not in the middle of a block comment
                if '*/' in line:
                    # If this line contains the end of a block comment, search for the comment start
                    comment_start = idx
                    while comment_start > start_idx:
                        if '/*' in lines[comment_start]:
                            # If the start of the comment is found, include up to before that
                            return comment_start
                        comment_start -= 1
                return min(idx + 1, original_end_idx)
            
            idx -= 1
        
        return original_end_idx
    
    def get_references_from_this(self) -> str:
        """
        Returns a string listing the symbols referenced by this symbol, one per line.
        Sorted by line_num_in_from, including file name and line number.
        
        Returns:
            str: Reference information string
        """
        conn = self._db.get_connection()
        
    # Get symbols referenced by this symbol
        results = conn.execute(f"""
            SELECT 
                sr.to_node,
                sr.line_num_in_from,
                sd.symbol_name,
                sd.file_path,
                sd.line_num_start
            FROM {REFERENCE_TABLE} sr
            JOIN {SYMBOL_TABLE} sd ON sr.to_node = sd.id
            WHERE sr.from_node = ?
            ORDER BY sr.line_num_in_from
        """, (self.id,)).fetchall()
        
        if not results:
            return "No references from this symbol"
        
    # Format results
        lines = []
        for to_node, line_num_in_from, symbol_name, file_path, line_num_start in results:
            # Extract only the file name
            filename = Path(file_path).name
            lines.append(
                f"{symbol_name:30s} at Line {line_num_in_from:5d}"
            )
        
        return '\n'.join(lines)
    
    def get_references_to_this(self) -> str:
        """
        Returns a string listing the symbols that reference this symbol, one per line.
        Includes file name and line number.
        
        Returns:
            str: Reference source information string
        """
        conn = self._db.get_connection()
        
    # Get symbols that reference this symbol
        results = conn.execute(f"""
            SELECT 
                sr.from_node,
                sr.line_num_in_from,
                sd.symbol_name,
                sd.file_path,
                sd.line_num_start
            FROM {REFERENCE_TABLE} sr
            JOIN {SYMBOL_TABLE} sd ON sr.from_node = sd.id
            WHERE sr.to_node = ?
            ORDER BY sd.file_path, sr.line_num_in_from
        """, (self.id,)).fetchall()
        
        if not results:
            return "No references to this symbol"
        
    # Format results
        lines = []
        for from_node, line_num_in_from, symbol_name, file_path, line_num_start in results:
            lines.append(
                f"{symbol_name:30s} at {file_path}:{line_num_in_from:5d}"
            )
        
        return '\n'.join(lines)
    
    def __str__(self) -> str:
        """String representation"""
        return (f"SNode(id={self.id}, symbol='{self.symbol_name}', "
                f"file='{self.file_path}', lines={self.line_num_start}-{self.line_num_end})")
    
    def __repr__(self) -> str:
        """Developer string representation"""
        return self.__str__()


# Utility functions
@lru_cache(maxsize=128)
def get_symbol_names() -> List[str]:
    """
    Get all unique symbol names in the database.
    
    Returns:
        List[str]: List of symbol names
    """
    db = DatabaseConnection()
    conn = db.get_connection()
    results = conn.execute(f"""
        SELECT DISTINCT symbol_name 
        FROM {SYMBOL_TABLE}
        ORDER BY symbol_name
    """).fetchall()
    
    return [row[0] for row in results]


def search_symbols(pattern: str) -> List[str]:
    """
    Search for symbol names matching the pattern.
    
    Args:
        pattern: Search pattern (SQL LIKE syntax)
    
    Returns:
        List[str]: List of matching symbol names
    """
    db = DatabaseConnection()
    conn = db.get_connection()
    results = conn.execute(f"""
        SELECT DISTINCT symbol_name 
        FROM {SYMBOL_TABLE}
        WHERE symbol_name LIKE ?
        ORDER BY symbol_name
    """, (pattern,)).fetchall()
    
    return [row[0] for row in results]


def get_symbol_by_file_and_line(file_path: str, line_num: int) -> Optional[SNode]:
    """
    Get a symbol by file path and line number.
    
    Args:
        file_path: File path
        line_num: Line number
    
    Returns:
        SNode: Corresponding symbol, or None if not found
    """
    db = DatabaseConnection()
    conn = db.get_connection()
    result = conn.execute(f"""
        SELECT id
        FROM {SYMBOL_TABLE}
        WHERE file_path = ?
          AND line_num_start <= ?
          AND (line_num_end >= ? OR line_num_end = 0)
        ORDER BY id
        LIMIT 1
    """, (file_path, line_num, line_num)).fetchone()
    
    if result:
        return SNode.from_id(result[0])
    return None


# Main function for testing
def main():
    """Main function for testing"""
    print("SNode Module Test")
    print("=" * 60)
    
    # Get list of symbol names
    symbols = get_symbol_names()
    print(f"Total unique symbols: {len(symbols)}")
    
    # Search for a sample symbol
    if symbols:
        # Test with the first symbol
        test_symbol = symbols[0]
        print(f"\nTesting with symbol: {test_symbol}")
        
        # Create SNode object
        node = SNode(test_symbol)
        print(f"Created: {node}")
        
        # Get source code
        try:
            code = node.get_source_code()
            print(f"\nSource code (first 200 chars):")
            print(code[:200] + "..." if len(code) > 200 else code)
        except Exception as e:
            print(f"Error getting source code: {e}")
        
        # Display reference information
        print("\n--- References FROM this symbol ---")
        print(node.get_references_from_this())
        
        print("\n--- References TO this symbol ---")
        print(node.get_references_to_this())
        
        # Test creation from ID
        print("\n" + "=" * 60)
        print("Testing factory function from_id...")
        node2 = SNode.from_id(node.id)
        print(f"Created from ID {node.id}: {node2}")


if __name__ == "__main__":
    main()
