"""
HTML parsing utilities using BeautifulSoup.
Author: gabes-machado
Created: 2025-01-17 01:42:33 UTC
Updated: 2025-01-19 20:04:33 UTC
"""

from bs4 import BeautifulSoup, Tag
from typing import Iterator, Tuple, Optional, Dict, Pattern
import re
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class ElementType(Enum):
    """Types of constitutional elements"""
    PREAMBULO = "PREAMBULO"
    TITULO = "TITULO"
    CAPITULO = "CAPITULO"
    SECAO = "SECAO"
    SUBSECAO = "SUBSECAO"
    ARTIGO = "ARTIGO"
    PARAGRAFO = "PARAGRAFO"
    INCISO = "INCISO"
    ALINEA = "ALINEA"
    ADCT = "ADCT"

@dataclass
class RegexPatterns:
    """Compiled regex patterns for better performance"""
    ROMAN_NUMERAL: Pattern = re.compile(r'[IVXLCDM]+')
    ARTICLE: Pattern = re.compile(r'Art\.\s*(\d+)')
    PARAGRAPH: Pattern = re.compile(r'§\s*(\d+)')
    INCISO: Pattern = re.compile(r'^[IVXLCDM]+\s*[-–]')
    INCISO_NUMBER: Pattern = re.compile(r'^([IVXLCDM]+)')
    ALINEA: Pattern = re.compile(r'^[a-z]\)')
    ALINEA_LETTER: Pattern = re.compile(r'^([a-z])')
    ADCT: Pattern = re.compile(r'ATO\s+DAS\s+DISPOSIÇÕES\s+CONSTITUCIONAIS\s+TRANSITÓRIAS', re.IGNORECASE)

class HTMLParser:
    def __init__(self, html_content: str):
        """Initialize the HTML parser with content and patterns"""
        try:
            # Try UTF-8 first
            self.soup = BeautifulSoup(html_content, 'html.parser', from_encoding='utf-8')
            
            # Check if parsing was successful
            if not self._validate_content_encoding():
                # Try ISO-8859-1 if UTF-8 fails
                self.soup = BeautifulSoup(html_content, 'html.parser', from_encoding='iso-8859-1')
                
                if not self._validate_content_encoding():
                    logger.warning("Content encoding issues detected")
            
            self.patterns = RegexPatterns()
            self._validate_html_content()
            logger.info("HTML Parser initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize HTML parser: {e}")
            raise

    def _validate_content_encoding(self) -> bool:
        """Validate if the content was parsed with correct encoding"""
        sample_text = self.soup.get_text()[:1000]
        return not any('�' in text for text in sample_text)

    def _validate_html_content(self) -> None:
        """Validate the HTML content structure"""
        if not self.soup.find():
            raise ValueError("Empty or invalid HTML content")
        
        required_tags = ['p', 'font']
        for tag in required_tags:
            if not self.soup.find(tag):
                logger.warning(f"Missing required tag: {tag}")

    def remove_strike_tags(self) -> None:
        """Remove strike tags from HTML with validation"""
        try:
            strike_tags = self.soup.find_all('strike')
            count = len(strike_tags)
            for strike in strike_tags:
                strike.decompose()
            logger.info(f"Removed {count} strike tags")
        except Exception as e:
            logger.error(f"Error removing strike tags: {e}")

    def iter_constitutional_elements(self) -> Iterator[Tuple[str, Optional[str], Optional[str], str]]:
        """
        Iterate through constitutional elements with improved error handling
        
        Yields:
            Tuple[str, Optional[str], Optional[str], str]: (element_type, number, title, text)
        """
        try:
            # Process preâmbulo
            yield from self._process_preambulo()

            # Process all other elements
            for p in self.soup.find_all('p'):
                try:
                    yield from self._process_paragraph(p)
                except Exception as e:
                    logger.error(f"Error processing paragraph: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error in constitutional elements iteration: {e}")
            raise

    def _process_preambulo(self) -> Iterator[Tuple[str, Optional[str], Optional[str], str]]:
        """Process preâmbulo section"""
        try:
            preambulo = self.soup.find('font', face='Arial')
            if preambulo:
                text = self._clean_text(preambulo.get_text())
                if text:
                    logger.info("Found preâmbulo")
                    yield ElementType.PREAMBULO.value, None, None, text
        except Exception as e:
            logger.error(f"Error processing preâmbulo: {e}")

    def _process_paragraph(self, p: Tag) -> Iterator[Tuple[str, Optional[str], Optional[str], str]]:
        """Process a single paragraph with type detection"""
        text = self._clean_text(p.get_text())
        if not text:
            return

        logger.debug(f"Processing text: {text[:100]}...")

        # Define element checks in order of specificity
        element_checks = [
            (self._check_adct, ElementType.ADCT),
            (self._check_titulo, ElementType.TITULO),
            (self._check_capitulo, ElementType.CAPITULO),
            (self._check_secao, ElementType.SECAO),
            (self._check_subsecao, ElementType.SUBSECAO),
            (self._check_artigo, ElementType.ARTIGO),
            (self._check_paragrafo, ElementType.PARAGRAFO),
            (self._check_inciso, ElementType.INCISO),
            (self._check_alinea, ElementType.ALINEA)
        ]

        for check_func, element_type in element_checks:
            result = check_func(text, p)
            if result:
                number, title = result
                logger.info(f"Found {element_type.value} {number or ''}")
                yield element_type.value, number, title, text
                return

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not isinstance(text, str):
            return ""
        return " ".join(text.strip().split())

    def _check_adct(self, text: str, p: Tag) -> Optional[Tuple[Optional[str], Optional[str]]]:
        """Check if text is ADCT header"""
        if self.patterns.ADCT.search(text):
            return None, text
        return None

    def _check_structural_element(self, text: str, p: Tag, keyword: str) -> Optional[Tuple[Optional[str], Optional[str]]]:
        """Generic checker for structural elements (título, capítulo, seção, subseção)"""
        if keyword in text.upper():
            number = self._extract_roman_numeral(text)
            next_p = p.find_next_sibling('p')
            title = self._extract_title(next_p) if next_p else None
            return number, title
        return None

    def _check_titulo(self, text: str, p: Tag) -> Optional[Tuple[Optional[str], Optional[str]]]:
        return self._check_structural_element(text, p, 'TÍTULO')

    def _check_capitulo(self, text: str, p: Tag) -> Optional[Tuple[Optional[str], Optional[str]]]:
        return self._check_structural_element(text, p, 'CAPÍTULO')

    def _check_secao(self, text: str, p: Tag) -> Optional[Tuple[Optional[str], Optional[str]]]:
        return self._check_structural_element(text, p, 'SEÇÃO')

    def _check_subsecao(self, text: str, p: Tag) -> Optional[Tuple[Optional[str], Optional[str]]]:
        return self._check_structural_element(text, p, 'SUBSEÇÃO')

    def _check_artigo(self, text: str, p: Tag) -> Optional[Tuple[Optional[str], None]]:
        if self.patterns.ARTICLE.match(text):
            return self._extract_article_number(text), None
        return None

    def _check_paragrafo(self, text: str, p: Tag) -> Optional[Tuple[Optional[str], None]]:
        if text.startswith('§') or text.lower().startswith('parágrafo'):
            return self._extract_paragraph_number(text), None
        return None

    def _check_inciso(self, text: str, p: Tag) -> Optional[Tuple[Optional[str], None]]:
        if self._is_inciso(text):
            return self._extract_inciso_number(text), None
        return None

    def _check_alinea(self, text: str, p: Tag) -> Optional[Tuple[Optional[str], None]]:
        if self._is_alinea(text):
            return self._extract_alinea_letter(text), None
        return None

    def _extract_roman_numeral(self, text: str) -> Optional[str]:
        """Extract Roman numeral with validation"""
        try:
            match = self.patterns.ROMAN_NUMERAL.search(text)
            if match:
                numeral = match.group(0)
                if set(numeral).issubset(set('IVXLCDM')):
                    return numeral
                logger.warning(f"Invalid Roman numeral found: {numeral}")
        except Exception as e:
            logger.error(f"Error extracting Roman numeral: {e}")
        return None

    def _extract_title(self, p: Optional[Tag]) -> Optional[str]:
        """Extract title with validation"""
        try:
            if p and isinstance(p, Tag):
                text = self._clean_text(p.get_text())
                if text and not any(keyword in text.upper() for keyword in 
                    ['TÍTULO', 'CAPÍTULO', 'SEÇÃO', 'SUBSEÇÃO', 'ART.', 'ATO DAS DISPOSIÇÕES']):
                    return text
        except Exception as e:
            logger.error(f"Error extracting title: {e}")
        return None

    def _extract_article_number(self, text: str) -> Optional[str]:
        """Extract article number with validation"""
        try:
            match = self.patterns.ARTICLE.search(text)
            return match.group(1) if match else None
        except Exception as e:
            logger.error(f"Error extracting article number: {e}")
            return None

    def _extract_paragraph_number(self, text: str) -> Optional[str]:
        """Extract paragraph number with special case handling"""
        try:
            if 'único' in text.lower():
                return 'único'
            match = self.patterns.PARAGRAPH.search(text)
            return match.group(1) if match else None
        except Exception as e:
            logger.error(f"Error extracting paragraph number: {e}")
            return None

    def _is_inciso(self, text: str) -> bool:
        """Check if text is an inciso"""
        return bool(self.patterns.INCISO.match(text))

    def _extract_inciso_number(self, text: str) -> Optional[str]:
        """Extract inciso number with validation"""
        try:
            match = self.patterns.INCISO_NUMBER.match(text)
            return match.group(1) if match else None
        except Exception as e:
            logger.error(f"Error extracting inciso number: {e}")
            return None

    def _is_alinea(self, text: str) -> bool:
        """Check if text is an alínea"""
        return bool(self.patterns.ALINEA.match(text))

    def _extract_alinea_letter(self, text: str) -> Optional[str]:
        """Extract alínea letter with validation"""
        try:
            match = self.patterns.ALINEA_LETTER.match(text)
            return match.group(1) if match else None
        except Exception as e:
            logger.error(f"Error extracting alínea letter: {e}")
            return None