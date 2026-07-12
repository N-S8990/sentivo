"""ONNX-optimised FinBERT for fast CPU sentiment inference."""

import logging
from pathlib import Path
from typing import List

import onnxruntime as ort
import torch
from optimum.onnxruntime import ORTModelForSequenceClassification
from transformers import AutoTokenizer

from sentivo.sentiment_analysis.base import BaseSentimentAnalyzer

logger = logging.getLogger(__name__)


class OnnxFinBert(BaseSentimentAnalyzer):
    """FinBERT model exported to ONNX with dynamic quantisation.
    
    Designed for sub-100 ms inference on CPU by running batches
    through an optimised ONNX Runtime session.
    """

    def __init__(
        self,
        model_path: Path = Path("models/finbert_onnx_quantized_dynamic"),
        file_name="model_quantized.onnx",
        batch_size: int = 32,
    ):
        logger.info("Loading ONNX FinBERT from %s", model_path)

        session_opts = ort.SessionOptions()
        session_opts.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        session_opts.intra_op_num_threads = 4
        session_opts.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL

        self.model = ORTModelForSequenceClassification.from_pretrained(
            model_path,
            file_name=file_name,
            local_files_only=True,
            session_options=session_opts,
            provider="CPUExecutionProvider",
        )
        self.tokenizer = AutoTokenizer.from_pretrained(
            model_path, local_files_only=True
        )
        self.batch_size = batch_size
        logger.info("ONNX FinBERT ready.")

    def _tokenize(self, texts: str | list[str]):
        return self.tokenizer(
            texts, return_tensors="pt", truncation=True, padding=True
        )

    def predict(self, texts: str | list[str] | list[list[str]]) -> List:
        """Predict sentiment for one or more texts.
        Returns a list of probability arrays [pos, neg, neutral].
        """
        results = []
        if not texts:
            return results

        if isinstance(texts, str):
            texts = [texts]

        for i in range(0, len(texts), self.batch_size):
            batch = texts[i : i + self.batch_size]
            valid = [t for t in batch if isinstance(t, str) and t.strip()]
            if not valid:
                continue

            try:
                inputs = self._tokenize(valid)
                with torch.no_grad():
                    outputs = self.model(**inputs)
                    probs = torch.softmax(outputs.logits, dim=-1)
                results.extend(probs.cpu().numpy())
            except Exception as e:
                logger.warning("Batch inference error: %s", e)
                continue

        return results
