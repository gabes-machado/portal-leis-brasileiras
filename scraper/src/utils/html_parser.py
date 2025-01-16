"""
HTML parsing utilities using BeautifulSoup.
Created by: gabes-machado
Date: 2025-01-16
"""

from bs4 import BeautifulSoup
from typing import List

class HTMLParser:
    def __init__(self, html_content: str):
        self.soup = BeautifulSoup(html_content, "lxml")

    def remove_strike_tags(self) -> None:
        """Remove tags <strike> do HTML"""
        for strike_tag in self.soup.find_all("strike"):
            strike_tag.decompose()

    def get_paragraphs(self) -> List[str]:
        """Retorna lista de textos dos parágrafos"""
        paragraphs = self.soup.find_all("p")
        return [p.get_text(" ", strip=True) for p in paragraphs if p.get_text().strip()]