import pandas as pd
import re
import logging

# Konfiguracja logowania (Seniorzy to kochają - pokazuje, że dbasz o monitoring)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def normalize_text(text):
    """Czyści i standaryzuje tekst."""
    if pd.isna(text):
        return "Unknown"
    # Usunięcie białych znaków na końcach, zamiana na małe litery, usunięcie podwójnych spacji
    text = str(text).strip().lower()
    text = re.sub(r'\s+', ' ', text)
    return text.title()

def extract_weight(product_name):
    """Wyciąga gramaturę/pojemność i ujednolica do gramów (g) lub mililitrów (ml)."""
    # Szuka wzorców np. "1kg", "500 g", "1.5 l", "250G"
    match = re.search(r'([\d\.,]+)\s*(kg|g|l|ml)', product_name.lower())
    
    if match:
        value_str = match.group(1).replace(',', '.') # Zamiana polskiego przecinka na kropkę
        unit = match.group(2)
        
        try:
            value = float(value_str)
            if unit == 'kg':
                return f"{int(value * 1000)}g"
            elif unit == 'l':
                return f"{int(value * 1000)}ml"
            else:
                return f"{int(value)}{unit}"
        except ValueError:
            return None
    return "No Data"

def run_etl():
    logging.info("Rozpoczęcie procesu ETL...")
    
    # 1. EXTRACT (Pobranie danych)
    try:
        df = pd.read_csv('../data/raw_data.csv')
        logging.info(f"Wczytano {len(df)} rekordów.")
    except FileNotFoundError:
        logging.error("Nie znaleziono pliku źródłowego.")
        return

    # 2. TRANSFORM (Transformacja danych)
    logging.info("Czyszczenie nazw i kategorii...")
    df['clean_name'] = df['product_name'].apply(normalize_text)
    df['clean_category'] = df['category'].apply(normalize_text)
    
    logging.info("Ekstrakcja i normalizacja gramatury...")
    df['standardized_weight'] = df['product_name'].apply(extract_weight)
    
    # Identyfikacja błędów (np. literówki w znanych markach)
    df['brand'] = df['clean_name'].apply(lambda x: 'Lavazza' if 'Lavaz' in x else 'Unknown')
    
    # Stworzenie raportu odrzutów (Quarantine) - Rekruterzy zwracają na to wielką uwagę!
    quarantine_df = df[df['standardized_weight'] == 'No Data']
    clean_df = df[df['standardized_weight'] != 'No Data']

    # 3. LOAD (Zapis wyników)
    clean_df.to_csv('../data/clean_data.csv', index=False)
    quarantine_df.to_csv('../data/quarantine_report.csv', index=False)
    
    logging.info(f"Zakończono. Zapisano {len(clean_df)} czystych rekordów i {len(quarantine_df)} błędnych.")

if __name__ == "__main__":
    run_etl()