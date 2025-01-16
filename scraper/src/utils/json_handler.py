"""
JSON handling utilities for constitution data.
Created by: gabes-machado
Date: 2025-01-16
"""

import json
from pathlib import Path
from typing import Dict, Any
import pandas as pd

class JSONHandler:
    @staticmethod
    def df_to_nested_dict(df: pd.DataFrame) -> Dict[str, Any]:
        """Converte DataFrame para estrutura aninhada"""
        hierarchical_cols = [
            "titulo", "capitulo", "secao", "subsecao",
            "artigo", "paragrafo", "inciso", "alinea"
        ]

        map_keys = {
            "titulo": "titulos",
            "capitulo": "capitulos",
            "secao": "secoes",
            "subsecao": "subsecoes",
            "artigo": "artigos",
            "paragrafo": "paragrafos",
            "inciso": "incisos",
            "alinea": "alineas"
        }

        root = {}

        for _, row in df.iterrows():
            current_level = root
            row_dict = row.to_dict()

            # Determina último nível hierárquico
            used_cols = [c for c in hierarchical_cols if row_dict.get(c) is not None]
            numero_col = used_cols[-1] if used_cols else None
            numero_str = row_dict.get(numero_col) if numero_col else None

            # Cria entrada
            entry = {
                "classe": numero_col if numero_col else "texto",
                "numero": numero_str,
                "texto": row_dict["texto"]
            }

            # Constrói estrutura aninhada
            for col in hierarchical_cols:
                val = row_dict.get(col)
                if val is None:
                    break
                plural_key = map_keys[col]
                current_level.setdefault(plural_key, {})
                current_level[plural_key].setdefault(val, {})
                current_level = current_level[plural_key][val]

            current_level.setdefault("conteudo", []).append(entry)

        return root

    @staticmethod
    def save_json(data: Dict[str, Any], output_file: str) -> None:
        """Salva dados em arquivo JSON"""
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)