"""
JSON handling utilities for constitution data.
Author: gabes-machado
Created: 2025-01-17 01:46:45 UTC
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import pandas as pd
from json.decoder import JSONDecodeError

logger = logging.getLogger(__name__)

@dataclass
class HierarchyConfig:
    """Configuration for hierarchical structure"""
    COLUMNS: List[str] = (
        "titulo", "capitulo", "secao", "subsecao",
        "artigo", "paragrafo", "inciso", "alinea"
    )
    
    PLURAL_MAPPING: Dict[str, str] = {
        "titulo": "titulos",
        "capitulo": "capitulos",
        "secao": "secoes",
        "subsecao": "subsecoes",
        "artigo": "artigos",
        "paragrafo": "paragrafos",
        "inciso": "incisos",
        "alinea": "alineas"
    }

class JSONHandlerError(Exception):
    """Custom exception for JSON handling errors"""
    pass

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
                    cls._process_row(row, root, stats)
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
        required_columns = set(HierarchyConfig.COLUMNS + ["texto"])
        missing_columns = required_columns - set(df.columns)
        
        if missing_columns:
            raise JSONHandlerError(
                f"Missing required columns: {', '.join(missing_columns)}"
            )
        
        if df.empty:
            raise JSONHandlerError("DataFrame is empty")
        
        if df["texto"].isna().any():
            logger.warning("Found rows with missing text")

    @classmethod
    def _process_row(
        cls, 
        row: pd.Series, 
        root: Dict[str, Any], 
        stats: Dict[str, Any]
    ) -> None:
        """
        Process a single DataFrame row into the nested structure
        
        Args:
            row: DataFrame row to process
            root: Root dictionary to update
            stats: Statistics dictionary to update
        """
        current_level = root
        row_dict = row.to_dict()

        # Determine hierarchical structure
        used_cols = [
            c for c in HierarchyConfig.COLUMNS 
            if pd.notna(row_dict.get(c))
        ]
        
        if not used_cols:
            stats["empty_rows"] += 1
            return

        numero_col = used_cols[-1]
        numero_str = row_dict.get(numero_col)

        # Update statistics
        stats["processed_rows"] += 1
        stats["structure_elements"][numero_col] = (
            stats["structure_elements"].get(numero_col, 0) + 1
        )

        # Create content entry
        entry = cls._create_entry(numero_col, numero_str, row_dict["texto"])

        # Build nested structure
        current_level = cls._build_nested_structure(
            current_level, 
            row_dict, 
            used_cols
        )

        # Add content to current level
        current_level.setdefault("conteudo", []).append(entry)

    @staticmethod
    def _create_entry(
        numero_col: str, 
        numero_str: Optional[str], 
        texto: str
    ) -> Dict[str, Any]:
        """
        Create a content entry with validation
        
        Args:
            numero_col: Column name for the number
            numero_str: Number string value
            texto: Text content
            
        Returns:
            Dict[str, Any]: Entry dictionary
        """
        return {
            "classe": numero_col if numero_col else "texto",
            "numero": numero_str,
            "texto": texto.strip() if isinstance(texto, str) else texto
        }

    @staticmethod
    def _build_nested_structure(
        root: Dict[str, Any], 
        row_dict: Dict[str, Any], 
        used_cols: List[str]
    ) -> Dict[str, Any]:
        """
        Build nested dictionary structure for a row
        
        Args:
            root: Root dictionary to build from
            row_dict: Row data dictionary
            used_cols: List of columns used in hierarchy
            
        Returns:
            Dict[str, Any]: Current level dictionary
        """
        current_level = root
        
        for col in used_cols:
            val = row_dict.get(col)
            if pd.isna(val):
                break
                
            plural_key = HierarchyConfig.PLURAL_MAPPING[col]
            current_level.setdefault(plural_key, {})
            current_level[plural_key].setdefault(str(val), {})
            current_level = current_level[plural_key][str(val)]
            
        return current_level

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

        except (OSError, JSONDecodeError) as e:
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

            # Validate required keys
            for key in HierarchyConfig.PLURAL_MAPPING.values():
                if key in data and not isinstance(data[key], dict):
                    logger.error(f"'{key}' is not a dictionary")
                    return False

            logger.info(f"Successfully validated JSON file: {file_path}")
            return True

        except Exception as e:
            logger.error(f"Error validating JSON: {e}")
            return False