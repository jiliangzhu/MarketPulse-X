from __future__ import annotations

import os
from functools import lru_cache
from typing import List

try:  # pragma: no cover - optional dependency
    import numpy as np
except Exception:  # pragma: no cover - fallback
    np = None  # type: ignore

try:  # pragma: no cover - optional dependency
    from sentence_transformers import SentenceTransformer
except Exception:  # pragma: no cover - fallback
    SentenceTransformer = None  # type: ignore

from backend.utils.logging import get_logger


class EmbeddingModel:
    def __init__(self, model_name: str = "all-MiniLM-L6-v2") -> None:
        self.logger = get_logger("embedding-model")
        self.dim = 384
        self._model = None
        offline = os.getenv("HF_HUB_OFFLINE", "").lower() in {"1", "true", "yes"}
        if offline:
            self.logger.warning("embedding-offline-mode", extra={"model": model_name})
            SentenceTransformer_local = None
        else:
            SentenceTransformer_local = SentenceTransformer
        if SentenceTransformer is not None:
            try:
                self.logger.info("loading-embedding-model", extra={"model": model_name})
                self._model = (SentenceTransformer_local or SentenceTransformer)(model_name)
                test_vector = self._model.encode("MarketPulse-X")
                if np is not None and isinstance(test_vector, np.ndarray):
                    self.dim = len(test_vector)
                else:
                    self.dim = len(list(test_vector))
            except Exception as exc:  # pragma: no cover - fallback
                self.logger.warning("embedding-model-load-failed", extra={"error": str(exc)})
                self._model = None
        else:  # pragma: no cover - fallback
            self.logger.warning("sentence-transformers-missing", extra={"model": model_name})

    def encode(self, text: str) -> List[float]:
        if not text:
            return [0.0] * self.dim
        if self._model is not None:
            embedding = self._model.encode(text, normalize_embeddings=True)
            if np is not None and isinstance(embedding, np.ndarray):
                return embedding.tolist()
            return list(embedding)
        return self._hash_embedding(text)

    def _hash_embedding(self, text: str) -> List[float]:
        seed = sum(ord(ch) for ch in text)
        if np is not None:
            rng = np.random.default_rng(seed)
            vec = rng.normal(size=self.dim)
            norm = np.linalg.norm(vec)
            if norm > 0:
                vec = vec / norm
            return vec.tolist()
        import random

        random.seed(seed)
        vec = [random.uniform(-1, 1) for _ in range(self.dim)]
        length = sum(abs(v) for v in vec) or 1
        return [v / length for v in vec]


@lru_cache(maxsize=1)
def get_embedding_model() -> EmbeddingModel:
    return EmbeddingModel()
