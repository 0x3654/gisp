"""
title: product name normalizer
author: m.kabakov
description:
version: 1.1
"""
import re
from fastapi import FastAPI
from pydantic import BaseModel
from rutermextract import TermExtractor
from nltk.stem.snowball import RussianStemmer
import pymorphy2

term_extractor = TermExtractor()
stemmer = RussianStemmer()
morph = pymorphy2.MorphAnalyzer()
app = FastAPI()

class NormalizeRequest(BaseModel):
    text: str
    debug: bool = False  # по умолчанию выключено

@app.post("/normalize")
def normalize(req: NormalizeRequest):
    terms_info = []
    for t in term_extractor(req.text):
        term_text = t.normalized
        stemmed = " ".join(stemmer.stem(w) for w in re.findall(r"[а-яa-z0-9]+", term_text.lower()))
        morphs = [morph.parse(w)[0] for w in term_text.split()]
        tags = [m.tag.POS for m in morphs if hasattr(m.tag, "POS")]
        terms_info.append({
            "original": " ".join([w.text if hasattr(w, "text") else str(w) for w in getattr(t, "words", [])]),
            "normalized": t.normalized,
            "stems": stemmed,
            "count": getattr(t, "count", 1),
            "word_count": getattr(t, "word_count", len(getattr(t, "words", []))),
            "pos_tags": tags,
            "words": [m.normal_form for m in morphs],
        })

    stems = sorted(set([i["stems"] for i in terms_info if i["stems"]]))
    normalized_text = " + ".join(stems)

    if req.debug or SHOW_DEBUG_NORM_DEFAULT:
        return {"debug": terms_info, "normalized": normalized_text}
    else:
        return {"normalized": normalized_text}