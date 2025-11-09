from __future__ import annotations

from pathlib import Path
from typing import List

import joblib
import pandas as pd

from backend.utils.logging import get_logger


class MLModel:
    def __init__(self, model_path: Path) -> None:
        self.logger = get_logger("ml-model")
        self.model_path = model_path
        self.model = joblib.load(model_path)
        self.logger.info("ml-model-loaded", extra={"path": str(model_path)})

    def predict_proba_batch(self, features_df: pd.DataFrame) -> List[float]:
        if features_df.empty:
            return []
        predictions = self.model.predict_proba(features_df)
        if predictions.shape[1] == 1:
            return predictions[:, 0].tolist()
        return predictions[:, 1].tolist()
