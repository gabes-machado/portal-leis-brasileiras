"""
Text processing utilities for cleaning and extracting information from text.
Author: gabes-machado
Created: 2025-01-16 20:41:21 UTC
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
        """
        Retorna a primeira correspondência de 'pattern' em 'text'
        
        Args:
            pattern: The regex pattern to search for
            text: The text to search in
            
        Returns:
            Union[str, None]: Matched text or None if no match
        """
        match = re.search(pattern=pattern, string=text, flags=re.IGNORECASE)
        return match.group(0) if match else None

    @staticmethod
    def extract_roman_number(pattern: str, text: str) -> Union[int, str, None]:
        """
        Extrai e converte números romanos do texto
        
        Args:
            pattern: The regex pattern to search for roman numerals
            text: The text to search in
            
        Returns:
            Union[int, str, None]: Converted number, original text, or None
        """
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
        """
        Remove espaços duplicados e quebras de linha
        
        Args:
            text: Text to clean
            
        Returns:
            str: Cleaned text
        """
        return re.sub(r"\s+", " ", text).strip()