"""
Configuration management for MLB betting splits project
"""

from pathlib import Path

import toml


class Config:
    """Configuration loader and manager"""

    def __init__(self, config_file="config.toml"):
        # Get the project root directory (where config.toml is located)
        self.project_root = Path(__file__).parent.parent
        self.config_path = self.project_root / config_file

        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        # Load configuration
        self._config = toml.load(self.config_path)

    @property
    def database_path(self):
        """Get the database file path"""
        return self._config["database"]["path"]

    @property
    def schema_name(self):
        """Get the schema name"""
        return self._config["schema"]["name"]

    @property
    def mlb_betting_splits_table(self):
        """Get the main MLB betting splits table name"""
        return self._config["tables"]["mlb_betting_splits"]

    @property
    def legacy_splits_table(self):
        """Get the legacy splits table name"""
        return self._config["tables"]["legacy_splits"]

    @property
    def full_table_name(self):
        """Get the full table name with schema"""
        return f"{self.schema_name}.{self.mlb_betting_splits_table}"

    @property
    def sbd_source(self):
        """Get the SBD source identifier"""
        return self._config["data_sources"]["sbd"]

    @property
    def vsin_source(self):
        """Get the VSIN source identifier"""
        return self._config["data_sources"]["vsin"]

    @property
    def sbd_api_url(self):
        """Get the SBD API URL with books"""
        base_url = self._config["api"]["sbd_url"]
        books = ",".join(self._config["api"]["sbd_books"])
        return f"{base_url}?books={books}"

    def get_insert_query(self, split_type):
        """Get the appropriate INSERT query for a split type"""
        table_name = self.full_table_name

        # All split types use the same INSERT query now with the long format
        return f"""
            INSERT INTO {table_name} (
                game_id, home_team, away_team, game_datetime, split_type, last_updated, source, book,
                home_or_over_bets, home_or_over_bets_percentage, home_or_over_stake_percentage,
                away_or_under_bets, away_or_under_bets_percentage, away_or_under_stake_percentage,
                split_value, sharp_action, outcome
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """


# Global configuration instance
config = Config()
