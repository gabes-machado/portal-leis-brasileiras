"""
Text processing utilities for cleaning and extracting information from text.
Author: gabes-machado
Created: 2025-01-17 01:48:52 UTC
"""

import re
import logging
from typing import Union, Optional, Pattern, Dict, Set
from functools import lru_cache
import unicodedata

logger = logging.getLogger(__name__)

class TextProcessingError(Exception):
    """Custom exception for text processing errors"""
    pass

class TextProcessor:
    """Text processing utilities with caching and validation"""
    
    # Cached regex patterns for better performance
    _PATTERNS: Dict[str, Pattern] = {}
    
    # Valid Roman numeral characters and their combinations
    ROMAN_CHARS: Set[str] = {'I', 'V', 'X', 'L', 'C', 'D', 'M'}
    VALID_ROMAN_PAIRS = {
        'I': {'I', 'V', 'X'},
        'V': {'I'},
        'X': {'I', 'V', 'X', 'L', 'C'},
        'L': {'I', 'V', 'X'},
        'C': {'I', 'V', 'X', 'L', 'C', 'D', 'M'},
        'D': {'I', 'V', 'X', 'L', 'C'},
        'M': {'I', 'V', 'X', 'L', 'C', 'D', 'M'}
    }

    @classmethod
    def _get_compiled_pattern(cls, pattern: str) -> Pattern:
        """
        Get or create compiled regex pattern
        
        Args:
            pattern: Regex pattern string
            
        Returns:
            Pattern: Compiled regex pattern
            
        Raises:
            TextProcessingError: If pattern is invalid
        """
        try:
            if pattern not in cls._PATTERNS:
                cls._PATTERNS[pattern] = re.compile(pattern, re.IGNORECASE)
            return cls._PATTERNS[pattern]
        except re.error as e:
            logger.error(f"Invalid regex pattern '{pattern}': {e}")
            raise TextProcessingError(f"Invalid regex pattern: {e}")

    @classmethod
    def search_regex(cls, pattern: str, text: str) -> Optional[str]:
        """
        Search for pattern in text with validation and error handling
        
        Args:
            pattern: The regex pattern to search for
            text: The text to search in
            
        Returns:
            Optional[str]: Matched text or None if no match
            
        Raises:
            TextProcessingError: If text is invalid
        """
        try:
            if not isinstance(text, str):
                raise ValueError(f"Invalid text type: {type(text)}")
                
            if not text.strip():
                return None
                
            compiled_pattern = cls._get_compiled_pattern(pattern)
            match = compiled_pattern.search(text)
            
            if match:
                result = match.group(0)
                logger.debug(f"Found match '{result}' for pattern '{pattern}'")
                return result
                
            return None
            
        except Exception as e:
            logger.error(f"Error in regex search: {e}")
            raise TextProcessingError(f"Regex search failed: {e}")

    @classmethod
    def extract_roman_number(
        cls, 
        pattern: str, 
        text: str
    ) -> Optional[Union[int, str]]:
        """
        Extract and convert Roman numerals with validation
        
        Args:
            pattern: The regex pattern to search for roman numerals
            text: The text to search in
            
        Returns:
            Optional[Union[int, str]]: Converted number, original text, or None
            
        Raises:
            TextProcessingError: If text or pattern is invalid
        """
        try:
            found = cls.search_regex(pattern, text)
            if not found:
                return None

            # Extract just the Roman numeral part
            roman_part = re.search(r'[IVXLCDM]+', found.upper())
            if not roman_part:
                return None

            numeral = roman_part.group(0)
            
            # Validate Roman numeral
            if not cls._is_valid_roman_numeral(numeral):
                logger.warning(f"Invalid Roman numeral found: {numeral}")
                return found

            # Try to convert to integer
            try:
                return cls._roman_to_int(numeral)
            except ValueError as e:
                logger.warning(f"Could not convert Roman numeral {numeral}: {e}")
                return found

        except Exception as e:
            logger.error(f"Error extracting Roman numeral: {e}")
            raise TextProcessingError(f"Roman numeral extraction failed: {e}")

    @classmethod
    def _is_valid_roman_numeral(cls, numeral: str) -> bool:
        """
        Validate Roman numeral format
        
        Args:
            numeral: Roman numeral string
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not numeral or not set(numeral).issubset(cls.ROMAN_CHARS):
            return False

        # Check for valid character pairs
        for i in range(len(numeral) - 1):
            current = numeral[i]
            next_char = numeral[i + 1]
            if next_char not in cls.VALID_ROMAN_PAIRS[current]:
                return False

        return True

    @staticmethod
    @lru_cache(maxsize=128)
    def _roman_to_int(roman: str) -> int:
        """
        Convert Roman numeral to integer with caching
        
        Args:
            roman: Roman numeral string
            
        Returns:
            int: Converted number
            
        Raises:
            ValueError: If conversion fails
        """
        values = {
            'I': 1, 'V': 5, 'X': 10, 'L': 50,
            'C': 100, 'D': 500, 'M': 1000
        }
        
        total = 0
        prev_value = 0
        
        for char in reversed(roman):
            curr_value = values[char]
            if curr_value >= prev_value:
                total += curr_value
            else:
                total -= curr_value
            prev_value = curr_value
            
        return total

    @classmethod
    def clean_whitespace(cls, text: str) -> str:
        """
        Clean whitespace with improved handling
        
        Args:
            text: Text to clean
            
        Returns:
            str: Cleaned text
            
        Raises:
            TextProcessingError: If text is invalid
        """
        try:
            if not isinstance(text, str):
                raise ValueError(f"Invalid text type: {type(text)}")

            # Normalize unicode characters
            text = unicodedata.normalize('NFKC', text)
            
            # Replace various whitespace characters
            text = re.sub(r'[\s\u200b\u00a0]+', ' ', text)
            
            # Remove leading/trailing whitespace
            text = text.strip()
            
            # Log if text was significantly changed
            original_length = len(text)
            cleaned_length = len(text)
            if abs(original_length - cleaned_length) > 10:
                logger.debug(
                    f"Significant whitespace cleaning: "
                    f"{original_length} -> {cleaned_length} chars"
                )
                
            return text

        except Exception as e:
            logger.error(f"Error cleaning whitespace: {e}")
            raise TextProcessingError(f"Whitespace cleaning failed: {e}")

    @classmethod
    def normalize_text(cls, text: str) -> str:
        """
        Normalize text for consistent processing
        
        Args:
            text: Text to normalize
            
        Returns:
            str: Normalized text
            
        Raises:
            TextProcessingError: If text is invalid
        """
        try:
            if not isinstance(text, str):
                raise ValueError(f"Invalid text type: {type(text)}")

            # Normalize unicode form
            text = unicodedata.normalize('NFKC', text)
            
            # Replace similar characters
            replacements = {
                '"': '"',
                '"': '"',
                ''': "'",
                ''': "'",
                '–': '-',
                '—': '-',
                '…': '...',
            }
            
            for old, new in replacements.items():
                text = text.replace(old, new)

            # Remove zero-width spaces and other invisible characters
            text = re.sub(r'[\u200b\u200c\u200d\u2060\ufeff]', '', text)
            
            return text.strip()

        except Exception as e:
            logger.error(f"Error normalizing text: {e}")
            raise TextProcessingError(f"Text normalization failed: {e}")