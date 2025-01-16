"""
Data transformation utilities for processing constitution text.
Created by: gabes-machado
Date: 2025-01-16
"""

import pandas as pd
from typing import Dict, Any, List
from .text_processor import TextProcessor

class ConstitutionTransformer:
    def __init__(self):
        self.regex_map_roman = {
            "titulo": r"(?<=^T[ÍI]TULO\s*)[IVX]+",
            "capitulo": r"(?<=^CAP[IÍ]TULO\s*)[IVX]+",
            "secao": r"(?<=^SEÇÃO\s*)[IVX]+",
            "subsecao": r"(?<=^SUBSEÇÃO\s*)[IVX]+"
        }
        self.regex_map_generic = {
            "artigo": r"(?<=^Art\.?\s*)[0-9A-Zº\-]+",
            "paragrafo": r"(?<=^§\s*)[0-9A-Zº\-]+"
        }

    def create_dataframe(self, paragraphs: List[str]) -> pd.DataFrame:
        """Cria DataFrame inicial com os parágrafos"""
        df = pd.DataFrame({"texto": paragraphs})
        df["texto"] = df["texto"].apply(TextProcessor.clean_whitespace)
        return df

    def extract_hierarchical_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extrai estrutura hierárquica do texto"""
        # Extrai números romanos
        for col, pattern in self.regex_map_roman.items():
            df[col] = df["texto"].apply(
                lambda x: TextProcessor.extract_roman_number(pattern, x.upper())
            )

        # Extrai artigos e parágrafos
        for col, pattern in self.regex_map_generic.items():
            df[col] = df["texto"].apply(
                lambda x: TextProcessor.search_regex(pattern, x)
            )

        # Processa parágrafos únicos
        df["paragrafo"] = df.apply(
            lambda row: "único" if TextProcessor.search_regex(
                r"^Parágrafo\s+único", row["texto"]
            ) else row["paragrafo"],
            axis=1
        )

        # Extrai incisos e alíneas
        df["inciso"] = df["texto"].apply(
            lambda x: TextProcessor.search_regex(r"^[IVXLA-Z\d]+(?=\s*-)")
        )
        df["alinea"] = df["texto"].apply(
            lambda x: TextProcessor.search_regex(r"^[a-z]\)")
        )

        # Corrige casos especiais
        df.loc[df["inciso"] == "VIX", "inciso"] = "IX"

        # Preenche valores hierárquicos
        hierarchical_cols = [
            "titulo", "capitulo", "secao", "subsecao",
            "artigo", "paragrafo", "inciso", "alinea"
        ]
        df[hierarchical_cols] = df[hierarchical_cols].ffill()

        return df