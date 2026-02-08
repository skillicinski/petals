"""Embedding model configurations for entity resolution.

This module defines available embedding models and their configurations.
Different models trade off speed, size, and accuracy for entity matching.
"""

from dataclasses import dataclass


@dataclass
class ModelConfig:
    """Configuration for a sentence-transformer model.

    Attributes:
        name: Model identifier (for sentence-transformers)
        display_name: Human-readable name
        size_mb: Approximate model size in MB
        embedding_dim: Dimensionality of embeddings
        domain: Domain expertise (general, biomedical, etc.)
        description: Brief description of model characteristics
    """

    name: str
    display_name: str
    size_mb: int
    embedding_dim: int
    domain: str
    description: str


# Available models for entity resolution
AVAILABLE_MODELS = {
    "minilm": ModelConfig(
        name="all-MiniLM-L6-v2",
        display_name="MiniLM-L6 v2",
        size_mb=80,
        embedding_dim=384,
        domain="general",
        description="Fast general-purpose model. Good baseline, trained on diverse web text.",
    ),
    "mpnet": ModelConfig(
        name="all-mpnet-base-v2",
        display_name="MPNet Base v2",
        size_mb=420,
        embedding_dim=768,
        domain="general",
        description="More capable general model. Better semantic understanding than MiniLM.",
    ),
    "biobert": ModelConfig(
        name="dmis-lab/biobert-base-cased-v1.2",
        display_name="BioBERT v1.2",
        size_mb=420,
        embedding_dim=768,
        domain="biomedical",
        description="Trained on PubMed + PMC. Understands biomedical/pharmaceutical terminology.",
    ),
    "multi-qa": ModelConfig(
        name="multi-qa-mpnet-base-dot-v1",
        display_name="Multi-QA MPNet",
        size_mb=420,
        embedding_dim=768,
        domain="search",
        description="Optimized for semantic search and entity matching tasks.",
    ),
    "pubmedbert": ModelConfig(
        name="microsoft/BiomedNLP-PubMedBERT-base-uncased-abstract",
        display_name="PubMedBERT",
        size_mb=420,
        embedding_dim=768,
        domain="biomedical",
        description="Trained on PubMed abstracts. Strong biomedical domain knowledge.",
    ),
    "stsb-roberta": ModelConfig(
        name="sentence-transformers/stsb-roberta-large",
        display_name="STSB-RoBERTa Large",
        size_mb=1340,
        embedding_dim=1024,
        domain="semantic-similarity",
        description="Trained on Semantic Textual Similarity. Designed to detect if texts mean the same thing.",
    ),
    "paraphrase-mpnet": ModelConfig(
        name="sentence-transformers/paraphrase-mpnet-base-v2",
        display_name="Paraphrase-MPNet",
        size_mb=420,
        embedding_dim=768,
        domain="paraphrase",
        description="Trained for paraphrase detection. Excellent for name variations and abbreviations.",
    ),
}


def get_model_config(model_key: str) -> ModelConfig:
    """Get configuration for a model by key.

    Args:
        model_key: Model key (e.g., 'minilm', 'mpnet', 'biobert')

    Returns:
        ModelConfig for the specified model

    Raises:
        KeyError: If model_key not found
    """
    if model_key not in AVAILABLE_MODELS:
        available = ", ".join(AVAILABLE_MODELS.keys())
        raise KeyError(f"Unknown model: {model_key}. Available: {available}")

    return AVAILABLE_MODELS[model_key]


def list_models() -> dict[str, ModelConfig]:
    """Get all available model configurations.

    Returns:
        Dictionary mapping model keys to ModelConfig objects
    """
    return AVAILABLE_MODELS.copy()
