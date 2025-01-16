"""
Data transformation utilities for processing constitution text.
Author: gabes-machado
Created: 2025-01-16 20:43:42 UTC
"""

import pandas as pd
from typing import Dict, Any, List
from .text_processor import TextProcessor

class ConstitutionTransformer:
    def __init__(self):
        # Updated patterns with fixed-width look-behind
        self.regex_map_roman = {
            "titulo": r"TÍTULO\s+([IVX]+)",      # Changed from look-behind to capture group
            "capitulo": r"CAPÍTULO\s+([IVX]+)",  # Changed from look-behind to capture group
            "secao": r"SEÇÃO\s+([IVX]+)",        # Changed from look-behind to capture group
            "subsecao": r"SUBSEÇÃO\s+([IVX]+)"   # Changed from look-behind to capture group
        }
        self.regex_map_generic = {
            "artigo": r"Art\.\s*([0-9A-Zº\-]+)",  # Changed from look-behind to capture group
            "paragrafo": r"§\s*([0-9A-Zº\-]+)"    # Changed from look-behind to capture group
        }

    def create_dataframe(self, paragraphs: List[str]) -> pd.DataFrame:
        """
        Cria DataFrame inicial com os parágrafos
        
        Args:
            paragraphs: List of paragraph texts
            
        Returns:
            pd.DataFrame: DataFrame with cleaned paragraphs
        """
        df = pd.DataFrame({"texto": paragraphs})
        df["texto"] = df["texto"].apply(TextProcessor.clean_whitespace)
        return df

    def _extract_with_pattern(self, text: str, pattern: str) -> str:
        """
        Helper method to extract text using pattern with capture group
        
        Args:
            text: Text to search in
            pattern: Regex pattern with capture group
            
        Returns:
            str: Matched group or None
        """
        match = TextProcessor.search_regex(pattern, text)
        if match and '(' in pattern:  # If pattern has a capture group
            # Extract just the number part using a new regex search
            number_match = TextProcessor.search_regex(r'([IVX0-9A-Zº\-]+)$', match)
            return number_match if number_match else match
        return match

    def extract_hierarchical_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrai estrutura hierárquica do texto
        
        Args:
            df: DataFrame with text column
            
        Returns:
            pd.DataFrame: DataFrame with hierarchical structure columns
        """
        # Extract roman numerals
        for col, pattern in self.regex_map_roman.items():
            df[col] = df["texto"].apply(
                lambda x: TextProcessor.extract_roman_number(pattern, x.upper())
            )

        # Extract articles and paragraphs
        for col, pattern in self.regex_map_generic.items():
            df[col] = df["texto"].apply(
                lambda x: self._extract_with_pattern(x, pattern)
            )

        # Process unique paragraphs
        df["paragrafo"] = df.apply(
            lambda row: "único" if TextProcessor.search_regex(
                r"Parágrafo\s+único", row["texto"]
            ) else row["paragrafo"],
            axis=1
        )

        # Extract incisos and alíneas with simpler patterns
        df["inciso"] = df["texto"].apply(
            lambda x: TextProcessor.search_regex(r"^([IVXLC]+)(?=\s*[-–])", x)
        )
        df["alinea"] = df["texto"].apply(
            lambda x: TextProcessor.search_regex(r"^([a-z])\)", x)
        )

        # Fix special cases
        df.loc[df["inciso"] == "VIX", "inciso"] = "IX"

        # Fill hierarchical values
        hierarchical_cols = [
            "titulo", "capitulo", "secao", "subsecao",
            "artigo", "paragrafo", "inciso", "alinea"
        ]
        df[hierarchical_cols] = df[hierarchical_cols].ffill()

        return df