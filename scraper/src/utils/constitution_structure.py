"""
Constitutional structure representation and processing.
Author: gabes-machado
Created: 2025-01-17 01:38:26 UTC
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Set
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class StructureType(Enum):
    PREAMBULO = "preambulo"
    TITULO = "titulo"
    CAPITULO = "capitulo"
    SECAO = "secao"
    SUBSECAO = "subsecao"
    ARTIGO = "artigo"
    PARAGRAFO = "paragrafo"
    INCISO = "inciso"
    ALINEA = "alinea"

    @classmethod
    def get_hierarchy(cls) -> List['StructureType']:
        """Returns the constitutional hierarchy in order"""
        return [
            cls.PREAMBULO,
            cls.TITULO,
            cls.CAPITULO,
            cls.SECAO,
            cls.SUBSECAO,
            cls.ARTIGO,
            cls.PARAGRAFO,
            cls.INCISO,
            cls.ALINEA
        ]

@dataclass
class ConstitutionalElement:
    type: StructureType
    number: Optional[str] = None
    title: Optional[str] = None
    text: Optional[str] = None
    children: List['ConstitutionalElement'] = field(default_factory=list)
    parent: Optional['ConstitutionalElement'] = None

    def add_child(self, child: 'ConstitutionalElement') -> None:
        """Add a child element with validation"""
        if not self._validate_child(child):
            logger.warning(
                f"Invalid child relationship: {child.type.value} cannot be "
                f"child of {self.type.value}"
            )
            return
        
        child.parent = self
        self.children.append(child)
        logger.debug(
            f"Added {child.type.value} {child.number or ''} to "
            f"{self.type.value} {self.number or ''}"
        )

    def _validate_child(self, child: 'ConstitutionalElement') -> bool:
        """Validate parent-child relationship"""
        hierarchy = StructureType.get_hierarchy()
        parent_idx = hierarchy.index(self.type)
        child_idx = hierarchy.index(child.type)
        return child_idx > parent_idx

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with validation"""
        result = {
            "tipo": self.type.value,
            "numero": self.number,
            "titulo": self.title,
            "texto": self.text,
        }
        
        if self.children:
            result["elementos"] = [child.to_dict() for child in self.children]
        
        return result

    def __str__(self) -> str:
        """String representation for logging"""
        parts = [self.type.value]
        if self.number:
            parts.append(str(self.number))
        if self.title:
            parts.append(f'"{self.title}"')
        return " ".join(parts)

class ConstitutionProcessor:
    def __init__(self):
        self.current_structure: Dict[StructureType, ConstitutionalElement] = {}
        self.root = ConstitutionalElement(type=StructureType.PREAMBULO)
        self.processed_elements: Set[str] = set()
        logger.info("Initialized ConstitutionProcessor")

    def process_element(self, type_str: str, number: Optional[str], 
                       title: Optional[str], text: str) -> None:
        """Process a constitutional element with improved handling"""
        try:
            element_type = StructureType[type_str.upper()]
        except KeyError:
            logger.error(f"Invalid element type: {type_str}")
            return

        # Create element identifier for tracking
        element_id = f"{element_type.value}_{number or 'none'}"
        
        # Check for duplicate elements
        if element_id in self.processed_elements:
            logger.warning(f"Duplicate element detected: {element_id}")
            return
            
        element = ConstitutionalElement(
            type=element_type,
            number=number,
            title=title,
            text=text
        )

        logger.debug(f"Processing {element}")

        self._handle_element_placement(element)
        self.processed_elements.add(element_id)

    def _handle_element_placement(self, element: ConstitutionalElement) -> None:
        """Handle element placement in the hierarchy"""
        if element.type == StructureType.PREAMBULO:
            self.root = element
            return

        hierarchy = StructureType.get_hierarchy()
        parent_type = self._find_parent_type(element.type, hierarchy)
        
        if not parent_type:
            logger.warning(f"No parent type found for {element}")
            return

        parent = self._get_or_create_parent(parent_type, element.type)
        if parent:
            parent.add_child(element)
            self._update_current_structure(element)

    def _get_or_create_parent(
        self, 
        parent_type: StructureType, 
        child_type: StructureType
    ) -> Optional[ConstitutionalElement]:
        """Get existing parent or create placeholder if needed"""
        parent = self.current_structure.get(parent_type)
        
        if not parent:
            logger.debug(
                f"Creating placeholder {parent_type.value} for {child_type.value}"
            )
            parent = ConstitutionalElement(type=parent_type)
            self.current_structure[parent_type] = parent
            
            # Recursively handle parent's placement
            self._handle_element_placement(parent)
            
        return parent

    def _update_current_structure(self, element: ConstitutionalElement) -> None:
        """Update current structure and clear lower levels"""
        hierarchy = StructureType.get_hierarchy()
        element_index = hierarchy.index(element.type)
        
        # Clear any lower-level elements
        for lower_type in hierarchy[element_index + 1:]:
            if lower_type in self.current_structure:
                logger.debug(f"Clearing {lower_type.value} from current structure")
                del self.current_structure[lower_type]
        
        # Update current element
        self.current_structure[element.type] = element

    def _find_parent_type(
        self, 
        element_type: StructureType, 
        hierarchy: List[StructureType]
    ) -> Optional[StructureType]:
        """Find appropriate parent type in hierarchy"""
        try:
            element_index = hierarchy.index(element_type)
            if element_index > 0:
                return hierarchy[element_index - 1]
        except ValueError:
            logger.error(f"Element type {element_type} not found in hierarchy")
        return None

    def validate_structure(self) -> bool:
        """Validate the complete structure"""
        def validate_element(element: ConstitutionalElement, path: str = "") -> bool:
            current_path = f"{path}/{element}"
            
            # Validate hierarchy
            if element.children:
                child_types = set(child.type for child in element.children)
                hierarchy = StructureType.get_hierarchy()
                element_index = hierarchy.index(element.type)
                
                for child_type in child_types:
                    child_index = hierarchy.index(child_type)
                    if child_index <= element_index:
                        logger.error(
                            f"Invalid hierarchy at {current_path}: "
                            f"{child_type.value} cannot be child of {element.type.value}"
                        )
                        return False
                
                # Validate children recursively
                return all(
                    validate_element(child, current_path) 
                    for child in element.children
                )
            
            return True

        return validate_element(self.root)