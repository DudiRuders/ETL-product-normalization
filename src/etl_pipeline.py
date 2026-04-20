import pytest
import pandas as pd
import numpy as np

# Poprawiony import - teraz nazwy idealnie pasują do Twojego skryptu
from src.etl_pipeline import normalize_text, extract_weight

# --- TESTY DLA CZYSZCZENIA TEKSTU ---

@pytest.mark.parametrize("input_text, expected_text", [
    ("  mleko UHT  ", "Mleko Uht"),
    ("SER zolty Gouda", "Ser Zolty Gouda"),
    ("podwojna  spacja", "Podwojna Spacja"),
    (np.nan, "Unknown")  # Twoja funkcja bezpiecznie zwraca "Unknown" dla pustych pól
])
def test_normalize_text(input_text, expected_text):
    """Testuje usuwanie białych znaków, formatowanie wielkości liter i obsługę NaN."""
    assert normalize_text(input_text) == expected_text


# --- TESTY DLA EKSTRAKCJI GRAMATUR ---

@pytest.mark.parametrize("input_name, expected_weight", [
    ("Jogurt 500 g", "500g"),
    ("Mleko 0.5kg", "500g"),
    ("Woda 1.5 l", "1500ml"),
    ("Sok 250ml", "250ml"),
    ("Ser 250G", "250g"),
    ("Produkt bez wagi", "No Data")  # Twoja funkcja zwraca "No Data", gdy brak wagi
])
def test_extract_weight(input_name, expected_weight):
    """Testuje wyciąganie wartości za pomocą Regex i przeliczanie jednostek (np. kg na g)."""
    assert extract_weight(input_name) == expected_weight
