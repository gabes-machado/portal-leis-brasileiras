"""
JSON handling utilities for constitution data.
Author: gabes-machado
Created: 2025-01-17 02:08:18 UTC
"""

import json
import pandas as pd
from json.decoder import JSONDecodeError
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

class JSONHandlerError(Exception):
    """Custom exception for JSON handling errors"""
    pass

@dataclass
class HierarchyConfig:
    """Configuration for hierarchical structure"""
    COLUMNS: List[str] = field(default_factory=lambda: [
        "titulo", "capitulo", "secao", "subsecao",
        "artigo", "paragrafo", "inciso", "alinea"
    ])
    
    PLURAL_MAPPING: Dict[str, str] = field(default_factory=lambda: {
        "titulo": "titulos",
        "capitulo": "capitulos",
        "secao": "secoes",
        "subsecao": "subsecoes",
        "artigo": "artigos",
        "paragrafo": "paragrafos",
        "inciso": "incisos",
        "alinea": "alineas"
    })

    @classmethod
    def get_plural_mapping(cls) -> Dict[str, str]:
        """Get the plural mapping dictionary"""
        return cls().PLURAL_MAPPING

    @classmethod
    def get_columns(cls) -> List[str]:
        """Get the columns list"""
        return cls().COLUMNS

class JSONHandler:
    """Handler for JSON operations with validation and error handling"""
    
    @classmethod
    def df_to_nested_dict(cls, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Convert DataFrame to nested dictionary structure with validation
        
        Args:
            df: DataFrame containing constitutional text and structure
            
        Returns:
            Dict[str, Any]: Nested dictionary structure
            
        Raises:
            JSONHandlerError: If DataFrame structure is invalid
        """
        try:
            cls._validate_dataframe(df)
            root = {}
            
            # Track statistics for logging
            stats = {
                "total_rows": len(df),
                "processed_rows": 0,
                "empty_rows": 0,
                "structure_elements": {}
            }

            for idx, row in df.iterrows():
                try:
                    current_level = root
                    row_dict = row.to_dict()

                    # Determine last hierarchical level
                    used_cols = [
                        c for c in HierarchyConfig.get_columns() 
                        if pd.notna(row_dict.get(c))
                    ]
                    
                    if not used_cols:
                        stats["empty_rows"] += 1
                        continue

                    numero_col = used_cols[-1]
                    numero_str = row_dict.get(numero_col)

                    # Create entry
                    entry = {
                        "classe": numero_col,
                        "numero": numero_str,
                        "texto": row_dict["texto"]
                    }

                    # Build nested structure
                    for col in used_cols:
                        val = row_dict.get(col)
                        if pd.isna(val):
                            break
                            
                        plural_key = HierarchyConfig.get_plural_mapping()[col]
                        current_level.setdefault(plural_key, {})
                        current_level[plural_key].setdefault(str(val), {})
                        current_level = current_level[plural_key][str(val)]

                    # Add content
                    current_level.setdefault("conteudo", []).append(entry)
                    
                    # Update statistics
                    stats["processed_rows"] += 1
                    stats["structure_elements"][numero_col] = (
                        stats["structure_elements"].get(numero_col, 0) + 1
                    )

                except Exception as e:
                    logger.warning(f"Error processing row {idx}: {e}")
                    continue

            cls._log_processing_stats(stats)
            return root

        except Exception as e:
            logger.error(f"Error converting DataFrame to nested dict: {e}")
            raise JSONHandlerError(f"Conversion failed: {str(e)}") from e

    @staticmethod
    def _validate_dataframe(df: pd.DataFrame) -> None:
        """
        Validate DataFrame structure and content
        
        Args:
            df: DataFrame to validate
            
        Raises:
            JSONHandlerError: If validation fails
        """
        required_columns = set(HierarchyConfig.get_columns() + ["texto"])
        missing_columns = required_columns - set(df.columns)
        
        if missing_columns:
            raise JSONHandlerError(
                f"Missing required columns: {', '.join(missing_columns)}"
            )
        
        if df.empty:
            raise JSONHandlerError("DataFrame is empty")
        
        if df["texto"].isna().any():
            logger.warning("Found rows with missing text")

    @staticmethod
    def _log_processing_stats(stats: Dict[str, Any]) -> None:
        """Log processing statistics"""
        logger.info(f"Processed {stats['processed_rows']} of {stats['total_rows']} rows")
        logger.info(f"Found {stats['empty_rows']} empty rows")
        
        for element, count in stats["structure_elements"].items():
            logger.info(f"Found {count} {element} elements")

    @classmethod
    def save_json(cls, data: Dict[str, Any], output_file: str) -> None:
        """
        Save data to JSON file with validation and error handling
        
        Args:
            data: Data to save
            output_file: Output file path
            
        Raises:
            JSONHandlerError: If saving fails
        """
        try:
            # Validate data
            if not isinstance(data, dict):
                raise ValueError("Data must be a dictionary")

            # Create directory if needed
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Save with proper encoding and formatting
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(
                    data,
                    f,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True
                )

            # Verify file was written
            file_size = output_path.stat().st_size
            logger.info(
                f"Successfully saved JSON to {output_file} "
                f"(size: {file_size/1024:.2f} KB)"
            )

        except Exception as e:
            logger.error(f"Error saving JSON: {e}")
            raise JSONHandlerError(f"Failed to save JSON: {str(e)}") from e

    @classmethod
    def validate_json(cls, file_path: str) -> bool:
        """
        Validate JSON file structure and content
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Validate structure
            if not isinstance(data, dict):
                logger.error("Root element is not a dictionary")
                return False

            # Validate required keys using the mapping
            plural_mapping = HierarchyConfig.get_plural_mapping()
            for key in plural_mapping.values():
                if key in data and not isinstance(data[key], dict):
                    logger.error(f"'{key}' is not a dictionary")
                    return False

            logger.info(f"Successfully validated JSON file: {file_path}")
            return True

        except Exception as e:
            logger.error(f"Error validating JSON: {e}")
            return False