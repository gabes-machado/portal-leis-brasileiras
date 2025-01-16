"""
Text processing utilities for cleaning and extracting information from text.
Created by: gabes-machado
Date: 2025-01-16
"""

import re
from typing import Union

try:
    import roman
except ImportError:
    roman = None

class TextProcessor:
    @staticmethod
    def search_regex(pattern: str, text: str) -> Union[str, None]:
        """Retorna a primeira correspondência de 'pattern' em 'text'"""
        match = re.search(pattern, flags=re.IGNORECASE, pattern=pattern, string=text)
        return match.group(0) if match else None

    @staticmethod
    def extract_roman_number(pattern: str, text: str) -> Union[int, str, None]:
        """Extrai e converte números romanos do texto"""
        found = TextProcessor.search_regex(pattern, text)
        if not found:
            return None
        if roman:
            try:
                return roman.fromRoman(found.upper())
            except:
                pass
        return found

    @staticmethod
    def clean_whitespace(text: str) -> str:
        """Remove espaços duplicados e quebras de linha"""
        return re.sub(r"\s+", " ", text).strip()