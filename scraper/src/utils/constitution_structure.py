"""
Constitution structure handling utilities.
Author: gabes-machado
Created: 2025-01-17 02:22:37 UTC
Updated: 2025-01-19 19:59:45 UTC
"""

import logging
from typing import Dict, Optional, Any, List
from enum import Enum
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

class StructureType(Enum):
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
class ConstitutionalElement:
    """Represents a constitutional element with its content"""
    type: StructureType
    number: Optional[str] = None
    title: Optional[str] = None
    text: Optional[str] = None
    children: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    content: List[Dict[str, Any]] = field(default_factory=list)

    def add_child(self, key: str, element: 'ConstitutionalElement') -> None:
        """
        Add a child element
        
        Args:
            key: The key to identify the child (e.g., 'titulos/I', 'artigos/5')
            element: The constitutional element to add
        """
        parts = key.split('/')
        base_key = parts[0]
        
        if len(parts) > 1:
            number = parts[1]
            if base_key not in self.children:
                self.children[base_key] = {}
            if isinstance(self.children[base_key], dict):
                self.children[base_key][number] = element
        else:
            self.children[key] = element

    def add_content(self, content_type: str, number: Optional[str], text: str) -> None:
        """
        Add content to the element
        
        Args:
            content_type: Type of content (e.g., 'artigo', 'paragrafo')
            number: Number or identifier of the element
            text: The actual text content
        """
        self.content.append({
            "classe": content_type,
            "numero": number,
            "texto": text
        })

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert element to dictionary representation
        
        Returns:
            Dict[str, Any]: Dictionary representing the element and its children
        """
        result = {}
        
        # Handle preambulo specially
        if self.type == StructureType.PREAMBULO and self.content:
            result["preambulo"] = self.content
        else:
            # Add content if exists
            if self.content:
                result["conteudo"] = self.content

        # Add children if exists
        for key, value in self.children.items():
            if isinstance(value, dict):
                # Handle numbered elements
                child_dict = {}
                for num, child in value.items():
                    if isinstance(child, ConstitutionalElement):
                        child_data = child.to_dict()
                        if child_data:
                            child_dict[num] = child_data
                if child_dict:
                    result[key] = child_dict
            elif isinstance(value, ConstitutionalElement):
                child_dict = value.to_dict()
                if child_dict:
                    result[key] = child_dict

        return result

class ConstitutionProcessor:
    """Processor for constitutional structure"""
    
    def __init__(self):
        # Initialize root structure with preambulo and ADCT
        self.root = ConstitutionalElement(type=StructureType.PREAMBULO)
        self.adct = ConstitutionalElement(type=StructureType.ADCT)
        self.current_structure: Dict[StructureType, ConstitutionalElement] = {}
        logger.info("Initialized ConstitutionProcessor")

    def _get_hierarchy_level(self, element_type: StructureType) -> int:
        """
        Get the hierarchy level for a given element type
        
        Args:
            element_type: The type of constitutional element
            
        Returns:
            int: The hierarchy level (0-8)
        """
        hierarchy = {
            StructureType.PREAMBULO: 0,
            StructureType.TITULO: 1,
            StructureType.CAPITULO: 2,
            StructureType.SECAO: 3,
            StructureType.SUBSECAO: 4,
            StructureType.ARTIGO: 5,
            StructureType.PARAGRAFO: 6,
            StructureType.INCISO: 7,
            StructureType.ALINEA: 8,
            StructureType.ADCT: 1  # ADCT tem nível equivalente a título
        }
        return hierarchy[element_type]

    def _get_element_key(self, element: ConstitutionalElement) -> str:
        """
        Get the appropriate key for the element
        
        Args:
            element: The constitutional element
            
        Returns:
            str: The key to use in the structure
        """
        type_mapping = {
            StructureType.TITULO: "titulos",
            StructureType.CAPITULO: "capitulos",
            StructureType.SECAO: "secoes",
            StructureType.SUBSECAO: "subsecoes",
            StructureType.ARTIGO: "artigos",
            StructureType.PARAGRAFO: "paragrafos",
            StructureType.INCISO: "incisos",
            StructureType.ALINEA: "alineas",
            StructureType.ADCT: "adct"
        }

        base_key = type_mapping[element.type]
        
        # Return only base key for elements without number
        if not element.number:
            return base_key
            
        # For numbered elements, return in the correct format for the schema
        return f"{base_key}/{element.number}"

    def _update_structure(self, element: ConstitutionalElement) -> None:
        """
        Update current structure based on hierarchy level
        
        Args:
            element: The new element being added
        """
        current_level = self._get_hierarchy_level(element.type)
        
        # Remove higher level elements from current structure
        to_remove = [
            t for t in self.current_structure.keys()
            if self._get_hierarchy_level(t) >= current_level
        ]
        for t in to_remove:
            self.current_structure.pop(t)
        
        # Add new element to current structure
        self.current_structure[element.type] = element

    def _place_element(self, element: ConstitutionalElement) -> None:
        """
        Place element in the appropriate location in the structure
        
        Args:
            element: The element to place in the hierarchy
        """
        current_level = self._get_hierarchy_level(element.type)
        parent = self.root

        # Find the closest parent in the current structure
        for t, e in self.current_structure.items():
            if self._get_hierarchy_level(t) < current_level:
                parent = e

        # Create the appropriate key for the element
        key = self._get_element_key(element)
        
        # Add to parent's children
        parent.add_child(key, element)

    def process_element(self, type_str: str, number: Optional[str], title: Optional[str], text: str) -> None:
        """
        Process a constitutional element
        
        Args:
            type_str: Type of the element as string
            number: Number or identifier of the element
            title: Title of the element (if applicable)
            text: The actual text content
        """
        try:
            element_type = StructureType[type_str.upper()]
            
            # Special handling for preambulo
            if element_type == StructureType.PREAMBULO:
                self.root.add_content("preambulo", None, text)
                return

            # Special handling for ADCT
            if element_type == StructureType.ADCT:
                self.adct.add_content("adct", number, text)
                return

            # Create new element
            element = ConstitutionalElement(
                type=element_type,
                number=number,
                title=title
            )

            # Add the text as content
            element.add_content(
                element_type.value.lower(),
                number,
                text
            )

            # Update structure and place element
            self._update_structure(element)
            self._place_element(element)

            logger.debug(f"Processed {element_type.value} {number or ''}")

        except Exception as e:
            logger.error(f"Error processing element {type_str}: {e}")
            raise

    def get_result(self) -> Dict[str, Any]:
        """
        Get the final processed result
        
        Returns:
            Dict[str, Any]: The complete constitution structure as a dictionary
        """
        result = {}
        
        # Add preambulo
        preambulo_dict = self.root.to_dict()
        if "preambulo" in preambulo_dict:
            result["preambulo"] = preambulo_dict["preambulo"]
        
        # Add main content
        for key, value in self.root.children.items():
            if isinstance(value, dict) or isinstance(value, ConstitutionalElement):
                result[key] = value.to_dict() if isinstance(value, ConstitutionalElement) else value
        
        # Add ADCT to final result
        adct_dict = self.adct.to_dict()
        if adct_dict:
            result["adct"] = adct_dict
            
        return result