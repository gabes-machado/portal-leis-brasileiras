"""
Data transformation utilities for processing constitution text.
Author: gabes-machado
Created: 2025-01-17 01:40:48 UTC
"""

import pandas as pd
import logging
from typing import Dict, Any, List, Optional
from .text_processor import TextProcessor

logger = logging.getLogger(__name__)

class ConstitutionTransformer:
    def __init__(self):
        """Initialize the transformer with improved regex patterns"""
        # Patterns for structural elements with Roman numerals
        self.regex_map_roman = {
            "titulo": r"TÍTULO\s+([IVXLCDM]+)\s*[-–]?\s*",
            "capitulo": r"CAPÍTULO\s+([IVXLCDM]+)\s*[-–]?\s*",
            "secao": r"SEÇÃO\s+([IVXLCDM]+)\s*[-–]?\s*",
            "subsecao": r"SUBSEÇÃO\s+([IVXLCDM]+)\s*[-–]?\s*"
        }
        
        # Patterns for articles and paragraphs
        self.regex_map_generic = {
            "artigo": r"Art\.\s*([0-9A-Zº\-]+)[º°]?\s*[-–.]?\s*",
            "paragrafo": r"§\s*([0-9A-Zº\-]+)[º°]?\s*[-–.]?\s*"
        }
        
        # Additional patterns for special cases
        self.special_patterns = {
            "paragrafo_unico": r"Parágrafo\s+[úu]nico\s*[-–.]?\s*",
            "inciso": r"^([IVXLCDM]+)\s*[-–]\s*",
            "alinea": r"^([a-z])\)\s*",
            "preambulo": r"(?i)PREÂMBULO"
        }
        
        logger.info("Initialized ConstitutionTransformer with regex patterns")

    def create_dataframe(self, paragraphs: List[str]) -> pd.DataFrame:
        """
        Create initial DataFrame with paragraphs and basic cleaning
        
        Args:
            paragraphs: List of paragraph texts
            
        Returns:
            pd.DataFrame: DataFrame with cleaned paragraphs
        """
        try:
            df = pd.DataFrame({"texto": paragraphs})
            df["texto"] = df["texto"].apply(self._clean_and_validate_text)
            logger.info(f"Created DataFrame with {len(df)} paragraphs")
            return df
        except Exception as e:
            logger.error(f"Error creating DataFrame: {e}")
            raise

    def _clean_and_validate_text(self, text: str) -> str:
        """
        Clean and validate text with error handling
        
        Args:
            text: Raw text to clean
            
        Returns:
            str: Cleaned text
        """
        try:
            if not isinstance(text, str):
                logger.warning(f"Non-string input detected: {type(text)}")
                text = str(text)
            
            cleaned_text = TextProcessor.clean_whitespace(text)
            if not cleaned_text:
                logger.warning("Empty text after cleaning")
                
            return cleaned_text
        except Exception as e:
            logger.error(f"Error cleaning text: {e}")
            return ""

    def _extract_with_pattern(self, text: str, pattern: str) -> Optional[str]:
        """
        Extract text using pattern with improved error handling
        
        Args:
            text: Text to search in
            pattern: Regex pattern with capture group
            
        Returns:
            Optional[str]: Matched group or None
        """
        try:
            if not text or not pattern:
                return None
                
            match = TextProcessor.search_regex(pattern, text)
            if not match:
                return None
                
            if '(' in pattern:
                number_match = TextProcessor.search_regex(r'([IVXLCDM0-9A-Zº\-]+)$', match)
                return number_match if number_match else match
                
            return match
        except Exception as e:
            logger.error(f"Error extracting pattern '{pattern}': {e}")
            return None

    def extract_hierarchical_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract hierarchical structure from text with validation
        
        Args:
            df: DataFrame with text column
            
        Returns:
            pd.DataFrame: DataFrame with hierarchical structure columns
        """
        try:
            if "texto" not in df.columns:
                raise ValueError("DataFrame must contain 'texto' column")

            # Initialize columns
            hierarchical_cols = [
                "titulo", "capitulo", "secao", "subsecao",
                "artigo", "paragrafo", "inciso", "alinea"
            ]
            for col in hierarchical_cols:
                df[col] = None

            # Extract preâmbulo
            df["is_preambulo"] = df["texto"].str.contains(
                self.special_patterns["preambulo"], 
                case=False, 
                regex=True
            )

            # Extract roman numeral elements
            for col, pattern in self.regex_map_roman.items():
                logger.debug(f"Extracting {col} using pattern: {pattern}")
                df[col] = df["texto"].apply(
                    lambda x: self._safe_extract_roman(pattern, x)
                )

            # Extract articles and paragraphs
            for col, pattern in self.regex_map_generic.items():
                logger.debug(f"Extracting {col} using pattern: {pattern}")
                df[col] = df["texto"].apply(
                    lambda x: self._extract_with_pattern(x, pattern)
                )

            # Process unique paragraphs
            df["paragrafo"] = df.apply(
                lambda row: self._handle_paragraph(row["texto"], row["paragrafo"]),
                axis=1
            )

            # Extract incisos and alíneas
            df["inciso"] = df["texto"].apply(
                lambda x: self._safe_extract_inciso(x)
            )
            df["alinea"] = df["texto"].apply(
                lambda x: self._safe_extract_alinea(x)
            )

            # Fix known special cases
            self._fix_special_cases(df)

            # Fill hierarchical values with validation
            df[hierarchical_cols] = self._safe_ffill(df[hierarchical_cols])

            logger.info("Successfully extracted hierarchical structure")
            return df

        except Exception as e:
            logger.error(f"Error extracting hierarchical structure: {e}")
            raise

    def _safe_extract_roman(self, pattern: str, text: str) -> Optional[str]:
        """Safely extract Roman numerals with validation"""
        try:
            if not isinstance(text, str):
                return None
            text = text.upper()
            result = TextProcessor.extract_roman_number(pattern, text)
            if result and not set(result).issubset(set('IVXLCDM')):
                logger.warning(f"Invalid Roman numeral detected: {result}")
                return None
            return result
        except Exception as e:
            logger.error(f"Error extracting Roman numeral: {e}")
            return None

    def _safe_extract_inciso(self, text: str) -> Optional[str]:
        """Safely extract inciso with validation"""
        try:
            if not isinstance(text, str):
                return None
            match = TextProcessor.search_regex(self.special_patterns["inciso"], text)
            if match and match == "VIX":  # Fix common OCR error
                return "IX"
            return match
        except Exception as e:
            logger.error(f"Error extracting inciso: {e}")
            return None

    def _safe_extract_alinea(self, text: str) -> Optional[str]:
        """Safely extract alínea with validation"""
        try:
            if not isinstance(text, str):
                return None
            match = TextProcessor.search_regex(self.special_patterns["alinea"], text)
            return match if match and match.islower() else None
        except Exception as e:
            logger.error(f"Error extracting alínea: {e}")
            return None

    def _handle_paragraph(self, text: str, current_para: Optional[str]) -> Optional[str]:
        """Handle paragraph extraction with special cases"""
        try:
            if TextProcessor.search_regex(self.special_patterns["paragrafo_unico"], text):
                return "único"
            return current_para
        except Exception as e:
            logger.error(f"Error handling paragraph: {e}")
            return current_para

    def _fix_special_cases(self, df: pd.DataFrame) -> None:
        """Fix known special cases and inconsistencies"""
        try:
            # Fix common OCR errors in Roman numerals
            df.loc[df["inciso"] == "VIX", "inciso"] = "IX"
            df.loc[df["inciso"] == "IIV", "inciso"] = "IV"
            
            # Add more special cases as needed
            
        except Exception as e:
            logger.error(f"Error fixing special cases: {e}")

    def _safe_ffill(self, df: pd.DataFrame) -> pd.DataFrame:
        """Safely forward fill values with validation"""
        try:
            return df.ffill()
        except Exception as e:
            logger.error(f"Error during forward fill: {e}")
            return df