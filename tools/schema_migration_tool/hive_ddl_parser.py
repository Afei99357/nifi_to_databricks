"""
Hive/Impala DDL Parser

Parses CREATE TABLE DDL statements from Hive/Impala
and extracts table structure information.
"""

import re
from typing import Dict, List, Optional, Tuple


class HiveDDLParser:
    """Parser for Hive/Impala CREATE TABLE statements."""

    def __init__(self, ddl: str):
        """
        Initialize parser with DDL statement.

        Args:
            ddl: The CREATE TABLE DDL string
        """
        self.ddl = ddl.strip()
        self.parsed_data: Dict = {}

    def parse(self) -> Dict:
        """
        Parse the DDL and extract all components.

        Returns:
            Dictionary with parsed table information
        """
        self.parsed_data = {
            "schema_name": self._extract_schema_name(),
            "table_name": self._extract_table_name(),
            "columns": self._extract_columns(),
            "partition_columns": self._extract_partition_columns(),
        }

        return self.parsed_data

    def _extract_schema_name(self) -> Optional[str]:
        """Extract schema/database name."""
        # Pattern: CREATE TABLE schema.table
        pattern = r"CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)"
        match = re.search(pattern, self.ddl, re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    def _extract_table_name(self) -> str:
        """Extract table name."""
        # Pattern: CREATE TABLE [schema.]table
        pattern = r"CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[a-zA-Z_][a-zA-Z0-9_]*\.)?([a-zA-Z_][a-zA-Z0-9_]*)"
        match = re.search(pattern, self.ddl, re.IGNORECASE)
        if match:
            return match.group(1)
        raise ValueError("Could not extract table name from DDL")

    def _extract_columns(self) -> List[Tuple[str, str]]:
        """
        Extract column definitions (excluding partition columns).

        Returns:
            List of (column_name, column_type) tuples
        """
        # Find the section between CREATE TABLE (...) and PARTITIONED BY
        pattern = (
            r"\(([^)]+(?:\([^)]*\)[^)]*)*)\)\s*(?:PARTITIONED BY|STORED AS|LOCATION|$)"
        )
        match = re.search(pattern, self.ddl, re.IGNORECASE | re.DOTALL)

        if not match:
            raise ValueError("Could not extract column definitions from DDL")

        columns_text = match.group(1)
        columns = []

        # Split by comma, but handle nested parentheses (e.g., DECIMAL(10,2))
        column_defs = self._split_by_comma(columns_text)

        for col_def in column_defs:
            col_def = col_def.strip()
            if not col_def:
                continue

            # Parse: column_name TYPE [COMMENT 'comment']
            parts = col_def.split(None, 1)
            if len(parts) >= 2:
                col_name = parts[0].strip()
                # Extract type (everything before COMMENT if exists)
                remaining = parts[1].strip()
                if "COMMENT" in remaining.upper():
                    col_type = remaining.split("COMMENT", 1)[0].strip()
                else:
                    col_type = remaining.strip()

                # Clean up type
                col_type = col_type.strip("'\"")

                columns.append((col_name, col_type))

        return columns

    def _extract_partition_columns(self) -> List[Tuple[str, str]]:
        """
        Extract partition column definitions.

        Returns:
            List of (partition_column_name, column_type) tuples
        """
        # Pattern: PARTITIONED BY (...)
        pattern = r"PARTITIONED\s+BY\s*\(([^)]+)\)"
        match = re.search(pattern, self.ddl, re.IGNORECASE | re.DOTALL)

        if not match:
            return []

        partition_text = match.group(1)
        partition_columns = []

        # Split by comma
        part_defs = self._split_by_comma(partition_text)

        for part_def in part_defs:
            part_def = part_def.strip()
            if not part_def:
                continue

            # Parse: column_name TYPE
            parts = part_def.split(None, 1)
            if len(parts) >= 2:
                col_name = parts[0].strip()
                col_type = parts[1].strip()
                # Remove COMMENT if present
                if "COMMENT" in col_type.upper():
                    col_type = col_type.split("COMMENT", 1)[0].strip()
                col_type = col_type.strip("'\"")
                partition_columns.append((col_name, col_type))

        return partition_columns

    def _split_by_comma(self, text: str) -> List[str]:
        """
        Split text by comma, handling nested parentheses.

        Args:
            text: Text to split

        Returns:
            List of split parts
        """
        parts = []
        current = []
        paren_depth = 0

        for char in text:
            if char == "(":
                paren_depth += 1
                current.append(char)
            elif char == ")":
                paren_depth -= 1
                current.append(char)
            elif char == "," and paren_depth == 0:
                parts.append("".join(current))
                current = []
            else:
                current.append(char)

        # Add the last part
        if current:
            parts.append("".join(current))

        return parts

    def get_full_table_name(self) -> str:
        """Get fully qualified table name."""
        if self.parsed_data.get("schema_name"):
            return f"{self.parsed_data['schema_name']}.{self.parsed_data['table_name']}"
        return self.parsed_data["table_name"]

    def summary(self) -> str:
        """Generate a summary of the parsed DDL."""
        if not self.parsed_data:
            self.parse()

        lines = [
            "=" * 60,
            "Parsed Table Information",
            "=" * 60,
            f"Table: {self.get_full_table_name()}",
            f"\nColumns ({len(self.parsed_data['columns'])}):",
        ]

        for col_name, col_type in self.parsed_data["columns"]:
            lines.append(f"  - {col_name}: {col_type}")

        if self.parsed_data["partition_columns"]:
            lines.append(
                f"\nPartition Columns ({len(self.parsed_data['partition_columns'])}):"
            )
            for col_name, col_type in self.parsed_data["partition_columns"]:
                lines.append(f"  - {col_name}: {col_type}")

        lines.append("=" * 60)

        return "\n".join(lines)


def parse_hive_ddl(ddl: str) -> Dict:
    """
    Parse Hive/Impala DDL statement.

    Args:
        ddl: The CREATE TABLE DDL string

    Returns:
        Dictionary with parsed table information:
        - schema_name: Schema/database name (optional)
        - table_name: Table name
        - columns: List of (column_name, column_type) tuples
        - partition_columns: List of (partition_column_name, column_type) tuples

    Example:
        >>> ddl = '''
        ... CREATE EXTERNAL TABLE my_schema.my_table (
        ...   id INT,
        ...   name STRING
        ... )
        ... PARTITIONED BY (date STRING)
        ... STORED AS PARQUET
        ... LOCATION 'hdfs://namenode/path'
        ... '''
        >>> parsed = parse_hive_ddl(ddl)
        >>> print(parsed['table_name'])
        my_table
    """
    parser = HiveDDLParser(ddl)
    return parser.parse()


if __name__ == "__main__":
    # Example usage
    example_ddl = """
    CREATE EXTERNAL TABLE obf_schema.obf_table_raw (
      col_a STRING,
      col_b_ts STRING,
      col_c_ts STRING,
      col_d INT
    )
    PARTITIONED BY (
      part_a_ts STRING,
      part_b_ts STRING
    )
    STORED AS PARQUET
    LOCATION 'hdfs://files-dev-server-1/user/hive/warehouse/obf_tables/obf_schema/obf_table_name_raw'
    """

    parser = HiveDDLParser(example_ddl)
    result = parser.parse()

    print(parser.summary())
    print(f"\nParsed {len(result['columns'])} columns")
    print(f"Parsed {len(result['partition_columns'])} partition columns")
