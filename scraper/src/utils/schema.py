"""
JSON Schema definition and validation for the Brazilian Constitution.
Author: gabes-machado
Created: 2025-01-19 19:52:06 UTC
"""

import json
import logging
from typing import Dict, Any
from pathlib import Path
from jsonschema import validate, ValidationError

logger = logging.getLogger(__name__)

# Schema definition for the Brazilian Constitution
CONSTITUTION_SCHEMA = {
    "type": "object",
    "required": ["preambulo", "titulos", "adct"],
    "properties": {
        "preambulo": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["classe", "texto"],
                "properties": {
                    "classe": {"type": "string", "enum": ["preambulo"]},
                    "numero": {"type": ["string", "null"]},
                    "texto": {"type": "string"}
                }
            }
        },
        "titulos": {
            "type": "object",
            "patternProperties": {
                "^[IVXLCDM]+$": {  # Padrão para números romanos
                    "type": "object",
                    "properties": {
                        "conteudo": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["classe", "numero", "texto"],
                                "properties": {
                                    "classe": {"type": "string", "enum": ["titulo"]},
                                    "numero": {"type": "string"},
                                    "texto": {"type": "string"}
                                }
                            }
                        },
                        "capitulos": {
                            "type": "object",
                            "patternProperties": {
                                "^[IVXLCDM]+$": {
                                    "type": "object",
                                    "properties": {
                                        "conteudo": {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "required": ["classe", "numero", "texto"],
                                                "properties": {
                                                    "classe": {"type": "string", "enum": ["capitulo"]},
                                                    "numero": {"type": "string"},
                                                    "texto": {"type": "string"}
                                                }
                                            }
                                        },
                                        "secoes": {
                                            "type": "object",
                                            "patternProperties": {
                                                "^[IVXLCDM]+$": {
                                                    "type": "object",
                                                    "properties": {
                                                        "conteudo": {
                                                            "type": "array",
                                                            "items": {
                                                                "type": "object",
                                                                "required": ["classe", "numero", "texto"],
                                                                "properties": {
                                                                    "classe": {"type": "string", "enum": ["secao"]},
                                                                    "numero": {"type": "string"},
                                                                    "texto": {"type": "string"}
                                                                }
                                                            }
                                                        },
                                                        "subsecoes": {
                                                            "type": "object",
                                                            "patternProperties": {
                                                                "^[IVXLCDM]+$": {
                                                                    "$ref": "#/definitions/dispositivos"
                                                                }
                                                            }
                                                        },
                                                        "artigos": {"$ref": "#/definitions/artigos"}
                                                    }
                                                }
                                            }
                                        },
                                        "artigos": {"$ref": "#/definitions/artigos"}
                                    }
                                }
                            }
                        },
                        "artigos": {"$ref": "#/definitions/artigos"}
                    }
                }
            }
        },
        "adct": {
            "type": "object",
            "properties": {
                "conteudo": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["classe", "texto"],
                        "properties": {
                            "classe": {"type": "string", "enum": ["adct"]},
                            "texto": {"type": "string"}
                        }
                    }
                },
                "artigos": {"$ref": "#/definitions/artigos"}
            }
        }
    },
    "definitions": {
        "artigos": {
            "type": "object",
            "patternProperties": {
                "^[0-9]+[A-Z]?$": {  # Permite artigos como "5" ou "5A"
                    "type": "object",
                    "properties": {
                        "conteudo": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["classe", "numero", "texto"],
                                "properties": {
                                    "classe": {"type": "string", "enum": ["artigo"]},
                                    "numero": {"type": "string"},
                                    "texto": {"type": "string"}
                                }
                            }
                        },
                        "paragrafos": {
                            "type": "object",
                            "patternProperties": {
                                "^[0-9]+|único$": {
                                    "type": "object",
                                    "properties": {
                                        "conteudo": {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "required": ["classe", "numero", "texto"],
                                                "properties": {
                                                    "classe": {"type": "string", "enum": ["paragrafo"]},
                                                    "numero": {"type": "string"},
                                                    "texto": {"type": "string"}
                                                }
                                            }
                                        },
                                        "incisos": {"$ref": "#/definitions/incisos"}
                                    }
                                }
                            }
                        },
                        "incisos": {"$ref": "#/definitions/incisos"}
                    }
                }
            }
        },
        "incisos": {
            "type": "object",
            "patternProperties": {
                "^[IVXLCDM]+$": {
                    "type": "object",
                    "properties": {
                        "conteudo": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["classe", "numero", "texto"],
                                "properties": {
                                    "classe": {"type": "string", "enum": ["inciso"]},
                                    "numero": {"type": "string"},
                                    "texto": {"type": "string"}
                                }
                            }
                        },
                        "alineas": {
                            "type": "object",
                            "patternProperties": {
                                "^[a-z]$": {
                                    "type": "object",
                                    "properties": {
                                        "conteudo": {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "required": ["classe", "numero", "texto"],
                                                "properties": {
                                                    "classe": {"type": "string", "enum": ["alinea"]},
                                                    "numero": {"type": "string"},
                                                    "texto": {"type": "string"}
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

class ConstitutionSchema:
    """Manager for the Brazilian Constitution JSON Schema"""
    
    def __init__(self):
        self.schema = CONSTITUTION_SCHEMA
    
    def validate_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate data against the Constitution schema
        
        Args:
            data: Dictionary containing the Constitution data
            
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            validate(instance=data, schema=self.schema)
            logger.info("Data validation successful")
            return True
            
        except ValidationError as e:
            logger.error(f"Schema validation error: {e.message}")
            logger.debug(f"Failed validating {e.instance} in {e.path}")
            return False
            
        except Exception as e:
            logger.error(f"Unexpected error during schema validation: {e}")
            return False