from __future__ import annotations

"""Parser factory for SportsbookReview.

Phase-3 requirement: allow dynamic parser selection based on page
structure (JSON-embedded vs plain HTML, future mobile templates, etc.).
For now we only have one concrete implementation – `SportsbookReviewParser` –
but wiring a factory now means the scraper doesn’t need to change when
new parsers are added later.
"""

import logging
from typing import Protocol, runtime_checkable

from .sportsbookreview_parser import SportsbookReviewParser

logger = logging.getLogger(__name__)


@runtime_checkable
class SupportsParseBetTypePage(Protocol):
    """Contract every parser must fulfil (subset of full interface)."""

    def parse_bet_type_page(
        self,
        html_content: str,
        bet_type: str,
        game_date,
        source_url: str,
    ) -> list:  # noqa: ANN401
        ...


class ParserFactory:
    """Return an appropriate parser for a given raw HTML blob."""

    _json_hint = '"gameView"'  # quick heuristic – present in JSON payload

    def __init__(self):
        # Cache singletons to avoid repeated instantiation cost
        self._parsers: dict[str, SupportsParseBetTypePage] = {
            "default": SportsbookReviewParser(),
        }

    def get_parser(self, html_content: str) -> SupportsParseBetTypePage:
        """Analyse *html_content* and return a parser instance."""
        # Very simple heuristic: if we can find the JSON hint quickly use the
        # default parser (it supports both JSON and HTML fallback). Future
        # specialised parsers can be added here.
        if self._json_hint in html_content[:10000]:  # inspect first 10k chars
            logger.debug("ParserFactory: selected JSON-aware parser")
            return self._parsers["default"]

        # Fallback – default parser still works (HTML mode)
        logger.debug("ParserFactory: default parser (HTML mode)")
        return self._parsers["default"]


# Convenience singleton
_parser_factory = ParserFactory()


def get_parser(html_content: str) -> SupportsParseBetTypePage:
    """Module-level helper mirroring factory singleton."""
    return _parser_factory.get_parser(html_content)
