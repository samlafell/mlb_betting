"""Team name mapping utilities."""

from ..models.game import Team


class TeamMapper:
    """Utility for mapping team names across different data sources."""

    # Team name mappings from various sources
    TEAM_MAPPINGS: dict[str, Team] = {
        # Team names (full names)
        "yankees": Team.NYY,
        "red sox": Team.BOS,
        "dodgers": Team.LAD,
        "astros": Team.HOU,
        "rays": Team.TB,
        "marlins": Team.MIA,
        "brewers": Team.MIL,
        "tigers": Team.DET,
        "twins": Team.MIN,
        "orioles": Team.BAL,
        "royals": Team.KC,
        "white sox": Team.CWS,
        "guardians": Team.CLE,
        "mariners": Team.SEA,
        "athletics": Team.OAK,
        "padres": Team.SD,
        "giants": Team.SF,
        "pirates": Team.PIT,
        "blue jays": Team.TOR,
        "rockies": Team.COL,
        "cubs": Team.CHC,
        "rangers": Team.TEX,
        "diamondbacks": Team.ARI,
        "phillies": Team.PHI,
        "nationals": Team.WSH,
        "angels": Team.LAA,
        "braves": Team.ATL,
        "mets": Team.NYM,
        "reds": Team.CIN,
        "cardinals": Team.STL,
        # Team abbreviations/codes
        "nyy": Team.NYY,
        "bos": Team.BOS,
        "lad": Team.LAD,
        "hou": Team.HOU,
        "tb": Team.TB,
        "mia": Team.MIA,
        "mil": Team.MIL,
        "det": Team.DET,
        "min": Team.MIN,
        "bal": Team.BAL,
        "kc": Team.KC,
        "cws": Team.CWS,
        "cle": Team.CLE,
        "sea": Team.SEA,
        "oak": Team.OAK,
        "sd": Team.SD,
        "sf": Team.SF,
        "pit": Team.PIT,
        "tor": Team.TOR,
        "col": Team.COL,
        "chc": Team.CHC,
        "tex": Team.TEX,
        "ari": Team.ARI,
        "phi": Team.PHI,
        "was": Team.WSH,
        "wsh": Team.WSH,
        "laa": Team.LAA,
        "atl": Team.ATL,
        "nym": Team.NYM,
        "cin": Team.CIN,
        "stl": Team.STL,
    }

    @classmethod
    def map_team_name(cls, name: str) -> Team | None:
        """Map team name string to Team enum."""
        normalized = name.lower().strip()
        return cls.TEAM_MAPPINGS.get(normalized)

    @classmethod
    def get_team_abbreviation(cls, team: Team) -> str:
        """Get standard team abbreviation."""
        # TODO: Implement abbreviation mapping
        return team.name[:3]

    @classmethod
    def add_mapping(cls, name: str, team: Team) -> None:
        """Add new team name mapping."""
        cls.TEAM_MAPPINGS[name.lower().strip()] = team


__all__ = ["TeamMapper"]
