"""
SQL Preprocessor for PostgreSQL compatibility.

This module provides SQL preprocessing to handle PostgreSQL-specific compatibility issues
for SQL compatibility, including function syntax, type casting, and column references.
"""

import logging
import re

logger = logging.getLogger(__name__)


class SQLPreprocessor:
    """Preprocesses SQL queries for PostgreSQL compatibility."""

    def __init__(self, database_type=None):
        """Initialize the SQL preprocessor."""
        self.database_type = database_type
        self.transformation_count = 0

    def process_sql_string(self, sql: str) -> str:
        """Process a SQL string through all transformations (legacy interface)."""
        return self.preprocess_sql(sql)

    def process_sql_file(self, file_path: str) -> str:
        """Process a SQL file through all transformations."""
        try:
            with open(file_path, encoding="utf-8") as f:
                sql_content = f.read()

            return self.preprocess_sql(sql_content)

        except FileNotFoundError:
            logger.error(f"SQL file not found: {file_path}")
            raise
        except OSError as e:
            logger.error(f"Error reading SQL file {file_path}: {e}")
            raise

    def preprocess_sql(self, sql: str) -> str:
        """
        Preprocess SQL query for PostgreSQL compatibility.

        Args:
            sql: The original SQL query

        Returns:
            The preprocessed SQL query
        """
        self.transformation_count = 0

        # Apply transformations in order of importance
        sql = self._fix_sql_comments(sql)
        sql = self._fix_round_function(sql)
        sql = self._fix_postgresql_types(sql)
        sql = self._fix_boolean_comparisons(sql)
        sql = self._handle_missing_columns(sql)
        sql = self._fix_strategy_performance_cte(sql)
        sql = self._fix_column_references(sql)
        sql = self._fix_aggregate_functions(sql)

        if self.transformation_count > 0:
            logger.debug(f"Applied {self.transformation_count} SQL transformations")

        return sql

    def _fix_sql_comments(self, sql: str) -> str:
        """Fix SQL comment syntax issues that cause parse errors."""
        # Fix "with sharp action detection" -> "-- with sharp action detection"
        # Look for comment-like text that's not properly commented
        patterns = [
            (
                r"\bwith\s+sharp\s+action\s+detection\b",
                "-- with sharp action detection",
            ),
            (
                r"\bwith\s+line\s+movement\s+analysis\b",
                "-- with line movement analysis",
            ),
            (
                r"\bwith\s+enhanced\s+timing\s+patterns\b",
                "-- with enhanced timing patterns",
            ),
        ]

        for pattern, replacement in patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
                self.transformation_count += 1
                logger.debug(f"Fixed SQL comment: {pattern} -> {replacement}")

        return sql

    def _fix_round_function(self, sql: str) -> str:
        """Fix ROUND function calls to include NUMERIC casting for PostgreSQL."""

        def find_matching_paren(text, start_pos):
            """Find the position of the matching closing parenthesis."""
            paren_count = 1
            pos = start_pos + 1

            while pos < len(text) and paren_count > 0:
                if text[pos] == "(":
                    paren_count += 1
                elif text[pos] == ")":
                    paren_count -= 1
                pos += 1

            return pos - 1 if paren_count == 0 else -1

        # Find ROUND function calls manually to handle nested parentheses correctly
        result_sql = sql
        offset = 0

        while True:
            # Find next ROUND function
            round_pos = result_sql.lower().find("round(", offset)
            if round_pos == -1:
                break

            # Find the matching closing parenthesis
            closing_paren_pos = find_matching_paren(
                result_sql, round_pos + 5
            )  # +5 for 'round'

            if closing_paren_pos == -1:
                offset = round_pos + 6
                continue

            # Extract the full ROUND function call
            full_round = result_sql[round_pos : closing_paren_pos + 1]
            content = full_round[6:-1]  # Remove 'ROUND(' and ')'

            # Find the last comma that separates expression from precision
            paren_count = 0
            last_comma_pos = -1

            for i, char in enumerate(content):
                if char == "(":
                    paren_count += 1
                elif char == ")":
                    paren_count -= 1
                elif char == "," and paren_count == 0:
                    last_comma_pos = i

            if last_comma_pos == -1:
                # No precision parameter, just cast the expression
                replacement = f"ROUND(({content})::NUMERIC)"
            else:
                # Split into expression and precision
                expression = content[:last_comma_pos].strip()
                precision = content[last_comma_pos + 1 :].strip()
                replacement = f"ROUND(({expression})::NUMERIC, {precision})"

            # Replace in the result
            result_sql = (
                result_sql[:round_pos]
                + replacement
                + result_sql[closing_paren_pos + 1 :]
            )

            self.transformation_count += 1
            logger.debug(f"Fixed ROUND function: {full_round} -> {replacement}")

            # Continue from after the replacement
            offset = round_pos + len(replacement)

        return result_sql

    def _fix_postgresql_types(self, sql: str) -> str:
        """Fix PostgreSQL type casting and function calls."""
        # Fix MAX/MIN with boolean fields - but be more specific about boolean fields
        boolean_field_patterns = [
            r"\b(MAX|MIN)\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*\.)?([a-zA-Z_]*(?:win|cover|over|under|sharp)[a-zA-Z0-9_]*)\s*\)",
            r"\b(MAX|MIN)\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*\.)?([a-zA-Z_]*(?:has_|is_)[a-zA-Z0-9_]*)\s*\)",
        ]

        for pattern in boolean_field_patterns:

            def replace_bool_agg(match):
                func = match.group(1)
                table_prefix = match.group(2) or ""
                field = match.group(3)

                # Skip date/time fields
                if any(
                    time_word in field.lower()
                    for time_word in ["date", "time", "datetime", "timestamp"]
                ):
                    return match.group(0)

                replacement = f"BOOL_OR({table_prefix}{field})"
                self.transformation_count += 1
                return replacement

            sql = re.sub(pattern, replace_bool_agg, sql, flags=re.IGNORECASE)

        return sql

    def _fix_boolean_comparisons(self, sql: str) -> str:
        """Fix boolean comparison operations."""
        # Fix integer = boolean comparisons
        patterns = [
            # Handle table-qualified boolean fields
            (
                r"\b([a-zA-Z_][a-zA-Z0-9_]*\.)?(home_win|home_cover_spread|over|under|has_reliable_volume|has_reverse_movement)\s*=\s*1\b",
                r"\1\2 = true",
            ),
            (
                r"\b([a-zA-Z_][a-zA-Z0-9_]*\.)?(home_win|home_cover_spread|over|under|has_reliable_volume|has_reverse_movement)\s*=\s*0\b",
                r"\1\2 = false",
            ),
            # Handle generic boolean patterns - but exclude row numbers and ranks
            (
                r"\b(?!rn\b|latest_rank\b|rank\b)([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*1\b(?=\s*(?:AND|OR|THEN|$))",
                r"\1 = true",
            ),
            (
                r"\b(?!rn\b|latest_rank\b|rank\b)([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*0\b(?=\s*(?:AND|OR|THEN|$))",
                r"\1 = false",
            ),
            # Fix overcorrection for numeric fields that should remain numeric
            (r"\b([a-zA-Z_]*credibility[a-zA-Z0-9_]*)\s*=\s*true\b", r"\1 = 1"),
            (r"\b([a-zA-Z_]*credibility[a-zA-Z0-9_]*)\s*=\s*false\b", r"\1 = 0"),
            (r"\b([a-zA-Z_]*score[a-zA-Z0-9_]*)\s*=\s*true\b", r"\1 = 1"),
            (r"\b([a-zA-Z_]*score[a-zA-Z0-9_]*)\s*=\s*false\b", r"\1 = 0"),
            (r"\b(rn|latest_rank|rank)\s*=\s*true\b", r"\1 = 1"),
            (r"\b(rn|latest_rank|rank)\s*=\s*false\b", r"\1 = 0"),
            (
                r"\b(direction_conflicts|fade_successful|consensus_follow_correct|consensus_fade_correct|has_reliable_volume|has_reverse_movement)\s*=\s*true\b",
                r"\1 = 1",
            ),
            (
                r"\b(direction_conflicts|fade_successful|consensus_follow_correct|consensus_fade_correct|has_reliable_volume|has_reverse_movement)\s*=\s*false\b",
                r"\1 = 0",
            ),
        ]

        for pattern, replacement in patterns:
            old_sql = sql
            sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
            if sql != old_sql:
                self.transformation_count += 1

        return sql

    def _handle_missing_columns(self, sql: str) -> str:
        """Handle missing column references."""
        # Fix game_datetime vs game_date column references
        patterns = [
            # Fix DATE casting of game_date to just use game_date directly
            (r"\bDATE\s*\(\s*go\.game_date\s*::[^)]+\)", "go.game_date"),
            # Remove problematic column references that don't exist
            (r"\bAND\s+validation_rate\s*[><=]+\s*\d+", ""),
            (r"\bAND\s+movement_validation_rate\s*[><=]+\s*\d+", ""),
            (r"\bAND\s+contrarian_rate\s*[><=]+\s*\d+", ""),
            (r"\bAND\s+ballpark_advantage_rate\s*[><=]+\s*\d+", ""),
            (r"\bAND\s+enhanced_strategy_rating\s+LIKE\s+[^)]+\)", ""),
            (r"\bWHERE\s+validation_rate\s*[><=]+\s*\d+\s+AND", "WHERE"),
            (r"\bWHERE\s+movement_validation_rate\s*[><=]+\s*\d+\s+AND", "WHERE"),
            (r"\bWHERE\s+contrarian_rate\s*[><=]+\s*\d+\s+AND", "WHERE"),
            (r"\bWHERE\s+ballpark_advantage_rate\s*[><=]+\s*\d+\s+AND", "WHERE"),
            (r"\bWHERE\s+enhanced_strategy_rating\s+LIKE\s+[^)]+\)\s+AND", "WHERE"),
            # Handle line_movement context issues - remove conditions that reference it when not available
            (r"\bAND\s+COALESCE\s*\(\s*line_movement[^)]*\)\s*[><=]+\s*\d+", ""),
            (
                r"\bAND\s+ABS\s*\(\s*COALESCE\s*\(\s*line_movement[^)]*\)\s*[><=]+\s*[\d.]+",
                "",
            ),
            (
                r"\bWHERE\s+COALESCE\s*\(\s*line_movement[^)]*\)\s*[><=]+\s*\d+\s+AND",
                "WHERE",
            ),
            # Handle specific line_movement patterns in CASE statements - replace with simpler conditions
            (
                r"ABS\s*\(\s*COALESCE\s*\(\s*line_movement,\s*0\s*\)\s*\)\s*<\s*[\d.]+",
                "1 = 1",
            ),
            (
                r"ABS\s*\(\s*COALESCE\s*\(\s*line_movement,\s*0\s*\)\s*\)\s*>\s*[\d.]+",
                "1 = 0",
            ),
            # Handle CASE statements with missing columns - replace conditions with always false
            (r"enhanced_strategy_rating\s+LIKE\s+'[^']+\'", "1 = 0"),
            (r"\(\s*ballpark_advantage_rate[^)]+\)", "(1 = 0)"),
            # Handle JSON split_value casting issues - replace with NULL when casting JSON to numeric
            (r"CAST\s*\(\s*split_value\s+AS\s+REAL\s*\)", "NULL::REAL"),
            (r"CAST\s*\(\s*prev_line\s+AS\s+REAL\s*\)", "NULL::REAL"),
        ]

        for pattern, replacement in patterns:
            old_sql = sql
            sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
            if sql != old_sql:
                self.transformation_count += 1

        return sql

    def _fix_strategy_performance_cte(self, sql: str) -> str:
        """Fix strategy_performance CTE column references."""
        # Look for strategy_performance CTE and fix column references within it
        if "strategy_performance AS" in sql:
            # In strategy_performance context, map home_or_over_bets + away_or_under_bets to total_bets_calc
            # This is a specific fix for CTEs that create calculated columns

            # Find the strategy_performance CTE section
            cte_pattern = r"(strategy_performance\s+AS\s*\([^)]*?\))"

            def fix_cte_columns(match):
                cte_content = match.group(1)

                # Replace column references in the final SELECT of the CTE
                cte_content = re.sub(
                    r"\bhome_or_over_bets\s*\+\s*away_or_under_bets\b",
                    "total_bets_calc",
                    cte_content,
                    flags=re.IGNORECASE,
                )

                # Also fix references to just total_bets in strategy_performance context
                cte_content = re.sub(
                    r"\btotal_bets\b(?!\s*as\s+)",
                    "total_bets_calc",
                    cte_content,
                    flags=re.IGNORECASE,
                )

                self.transformation_count += 1
                return cte_content

            sql = re.sub(
                cte_pattern, fix_cte_columns, sql, flags=re.IGNORECASE | re.DOTALL
            )

        return sql

    def _fix_column_references(self, sql: str) -> str:
        """Fix column reference issues in complex queries."""
        # Handle home_or_over_bets + away_or_under_bets in different contexts

        # First, check if we're in a context where these columns should exist
        # If the query directly selects from splits.raw_mlb_betting_splits, columns should exist
        if "FROM splits.raw_mlb_betting_splits" in sql:
            # In direct table access, the columns exist, so we don't need to transform
            # But we still need to handle COALESCE wrapping for safety
            patterns = [
                (
                    r"\b(home_or_over_bets\s*\+\s*away_or_under_bets)\b",
                    r"(COALESCE(home_or_over_bets, 0) + COALESCE(away_or_under_bets, 0))",
                ),
            ]
        else:
            # In CTE or subquery contexts, we might need different handling
            patterns = [
                (
                    r"\b(home_or_over_bets\s*\+\s*away_or_under_bets)\b",
                    r"(COALESCE(home_or_over_bets, 0) + COALESCE(away_or_under_bets, 0))",
                ),
            ]

        for pattern, replacement in patterns:
            old_sql = sql
            sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
            if sql != old_sql:
                self.transformation_count += 1

        return sql

    def _fix_aggregate_functions(self, sql: str) -> str:
        """Fix aggregate function usage."""
        # Fix MODE() function which doesn't exist in PostgreSQL
        # Replace with a subquery that gets the most frequent value
        mode_pattern = r"MODE\s*\(\s*([^)]+)\s*\)"

        def replace_mode(match):
            column = match.group(1).strip()
            # Use a subquery to get the most common value
            replacement = f"""(SELECT {column} 
                            FROM (SELECT {column}, COUNT(*) as cnt 
                                  FROM (VALUES {column}) as t({column}) 
                                  GROUP BY {column} 
                                  ORDER BY cnt DESC 
                                  LIMIT 1) as mode_result)"""
            self.transformation_count += 1
            return replacement

        sql = re.sub(mode_pattern, replace_mode, sql, flags=re.IGNORECASE)

        return sql

    def get_transformation_summary(self, original_sql: str, processed_sql: str) -> dict:
        """Generate a summary of transformations applied."""
        return {
            "original_length": len(original_sql),
            "processed_length": len(processed_sql),
            "table_replacements_applied": 0,
            "syntax_transformations_applied": self.transformation_count,
            "transformations_applied": self.transformation_count,
            "validation_issues": [],
        }
