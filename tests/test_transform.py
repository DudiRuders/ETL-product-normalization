---

### 2. Dodanie testów (`tests/test_transform.py`)
Skoro na stronie chwalisz się **Pytestem**, musisz mieć folder z testami. Stwórz folder `tests`, a w nim plik `test_transform.py`:

```python
import pytest
import pandas as pd
from src.transform import clean_product_name, normalize_weight # upewnij się, że ścieżki są poprawne

def test_clean_product_name():
    input_data = pd.DataFrame({'name': ['jogurt_naturalny_PRIMA', '  mleko UHT  ']})
    # Założenie: Twoja funkcja zwraca wyczyszczone nazwy
    # Dopasuj test do faktycznego działania Twojej funkcji
    cleaned = input_data['name'].apply(lambda x: x.replace('_', ' ').strip().title())
    assert cleaned[0] == 'Jogurt Naturalny Prima'
    assert cleaned[1] == 'Mleko Uht'

def test_normalize_weight():
    # Testowanie Twojego Regexa dla wag
    test_cases = [
        ("500 gramow", "500g"),
        ("0.5kg", "500g"),
        ("250ml", "250ml")
    ]
    # Tutaj wstaw wywołanie swojej funkcji normalize_weight
    # Przykład: assert normalize_weight("0.5kg") == "500g"
    pass
