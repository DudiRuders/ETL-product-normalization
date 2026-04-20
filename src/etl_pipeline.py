import pytest
import pandas as pd
import numpy as np

from src.etl_pipeline import normalize_text, extract_weight

@pytest.mark.parametrize("input_text, expected_text", [
    ("  mleko UHT  ", "Mleko Uht"),
    ("SER zolty Gouda", "Ser Zolty Gouda"),
    ("podwojna  spacja", "Podwojna Spacja"),
    (np.nan, "Unknown")
])
def test_normalize_text(input_text, expected_text):
    assert normalize_text(input_text) == expected_text

@pytest.mark.parametrize("input_name, expected_weight", [
    ("Jogurt 500 g", "500g"),
    ("Mleko 0.5kg", "500g"),
    ("Woda 1.5 l", "1500ml"),
    ("Sok 250ml", "250ml"),
    ("Ser 250G", "250g"),
    ("Produkt bez wagi", "No Data")
])
def test_extract_weight(input_name, expected_weight):
    assert extract_weight(input_name) == expected_weight
