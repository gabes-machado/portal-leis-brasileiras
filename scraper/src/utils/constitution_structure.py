"""
Constitution structure handling utilities.
Author: gabes-machado
Created: 2025-01-17 02:22:37 UTC
"""

import logging
from typing import Dict, Optional, Any, List
from enum import Enum, auto
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

@dataclass
class ConstitutionalElement:
    """Represents a constitutional element with its content"""
    type: StructureType
    number: Optional[str] = None
    title: Optional[str] = None
    text: Optional[str] = None
    children: Dict[str, 'ConstitutionalElement'] = field(default_factory=dict)
    content: List[Dict[str, Any]] = field(default_factory=list)

    def add_child(self, key: str, element: 'ConstitutionalElement') -> None:
        """Add a child element"""
        self.children[key] = element

    def add_content(self, content_type: str, number: Optional[str], text: str) -> None:
        """Add content to the element"""
        self.content.append({
            "classe": content_type,
            "numero": number,
            "texto": text
        })

    def to_dict(self) -> Dict[str, Any]:
        """Convert element to dictionary representation"""
        result = {}
        
        # Add content if exists
        if self.content:
            result["conteudo"] = self.content

        # Add children if exists
        for key, child in self.children.items():
            child_dict = child.to_dict()
            if child_dict:  # Only add non-empty children
                result[key] = child_dict

        return result

class ConstitutionProcessor:
    """Processor for constitutional structure"""
    
    def __init__(self):
        self.root = ConstitutionalElement(type=StructureType.PREAMBULO)
        self.current_structure: Dict[StructureType, ConstitutionalElement] = {}
        logger.info("Initialized ConstitutionProcessor")

    def _get_hierarchy_level(self, element_type: StructureType) -> int:
        """Get the hierarchy level for a given element type"""
        hierarchy = {
            StructureType.PREAMBULO: 0,
            StructureType.TITULO: 1,
            StructureType.CAPITULO: 2,
            StructureType.SECAO: 3,
            StructureType.SUBSECAO: 4,
            StructureType.ARTIGO: 5,
            StructureType.PARAGRAFO: 6,
            StructureType.INCISO: 7,
            StructureType.ALINEA: 8
        }
        return hierarchy[element_type]

    def process_element(self, type_str: str, number: Optional[str], title: Optional[str], text: str) -> None:
        """Process a constitutional element"""
        try:
            element_type = StructureType[type_str.upper()]
            
            # Special handling for preambulo
            if element_type == StructureType.PREAMBULO:
                self.root.add_content("preambulo", None, text)
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

    def _update_structure(self, element: ConstitutionalElement) -> None:
        """Update current structure based on hierarchy level"""
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
        """Place element in the appropriate location in the structure"""
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

    def _get_element_key(self, element: ConstitutionalElement) -> str:
        """Get the appropriate key for the element"""
        type_mapping = {
            StructureType.TITULO: "titulos",
            StructureType.CAPITULO: "capitulos",
            StructureType.SECAO: "secoes",
            StructureType.SUBSECAO: "subsecoes",
            StructureType.ARTIGO: "artigos",
            StructureType.PARAGRAFO: "paragrafos",
            StructureType.INCISO: "incisos",
            StructureType.ALINEA: "alineas"
        }

        base_key = type_mapping[element.type]
        return f"{base_key}/{element.number}" if element.number else base_key

    def get_result(self) -> Dict[str, Any]:
        """Get the final processed result"""
        return self.root.to_dict()