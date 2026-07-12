"""Standalone test of the FinBERT model on a few example sentences."""

import time

import torch
from torch.nn.functional import softmax
from transformers import AutoModelForSequenceClassification, AutoTokenizer

test_texts = [
    "Tesla just launched two new affordable models with limited changes.",
    "The stock rallied sharply on better-than-expected quarterly earnings.",
    "Regulatory hurdles continue to pose significant downside risk.",
]

print("Loading FinBERT...")
tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")

start = time.time()
inputs = tokenizer(test_texts, return_tensors="pt", truncation=True, padding=True)
outputs = model(**inputs)
probs = softmax(outputs.logits, dim=1)
elapsed = time.time() - start

print(f"Time: {elapsed:.3f}s")
for txt, prob in zip(test_texts, probs):
    pos, neg, neu = prob.tolist()
    print(f"  {txt[:50]:50s} → pos={pos:.3f} neg={neg:.3f} neu={neu:.3f}")
