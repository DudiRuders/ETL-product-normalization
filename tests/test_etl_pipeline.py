import pytest
import pandas as pd
import numpy as np

# Bezpośredni import Twoich funkcji z pliku src/etl_pipeline.py
from src.etl_pipeline import clean_product_name, normalize_weight


# --- TESTY DLA NAZW PRODUKTÓW ---

@pytest.mark.parametrize("input_name, expected_name", [
    ("jogurt_naturalny_PRIMA", "Jogurt Naturalny Prima"),
    ("  mleko_UHT  ", "Mleko Uht"),
    ("SER_zolty_Gouda", "Ser Zolty Gouda") 
])
def test_clean_product_name(input_name, expected_name):
    """Testuje usuwanie znaków specjalnych i formatowanie wielkości liter."""
    assert clean_product_name(input_name) == expected_name

def test_clean_product_name_nan():
    """Testuje zabezpieczenie funkcji przed pustymi komórkami (NaN)."""
    assert pd.isna(clean_product_name(np.nan))


# --- TESTY DLA GRAMATUR (REGEX I MATEMATYKA) ---

@pytest.mark.parametrize("input_weight, expected_weight", [
    ("500 gramow", "500g"),
    ("0.5kg", "500g"),
    ("0.5 kg", "500g"),
    ("1.5 Kg", "1500g"),
    ("250ml", "250ml"),
    ("1 Kilo", "1000g")
])
def test_normalize_weight(input_weight, expected_weight):
    """Testuje wyciąganie wartości i przeliczanie jednostek (np. kg na g)."""
    assert normalize_weight(input_weight) == expected_weight

def test_normalize_weight_nan():
    """Testuje zabezpieczenie funkcji przed pustymi komórkami (NaN)."""
    assert pd.isna(normalize_weight(np.nan))
