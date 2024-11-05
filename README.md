# Order-to-Cash Datensatz für die Anwendung von Case-centric und Object-centric Process Mining
Dieses Repository enthält Python Skripte zur Generierung eines Object-centric Process Mining Datensatzes und dessen Abflachung für die Anwendung von Case-centric Process Mining.
Der Datensatz bildet einen Order-to-Cash Prozess ab, welcher einen realitätsnahen Geschäftsprozess durch die Erstellung von Objekten, Events und Change Events simuliert.
Diese Daten können für die Analyse mit Object-centric und Case-centruc Process Mining genutzt werden. 

## Voraussetzungen
- Python-Version: Python 3.8 oder höher
- Abhängigkeiten: 
  - `pandas` für die Datenmanipulation und Verwaltung
  - `numpy` für numerische Berechnungen und Zufallsgenerierung
  - `datetime` (Standardbibliothek) für die Verwaltung von Zeitstempeln

- Installiere die Pakete mit folgendem Befehl:

  ```bash
  pip install pandas numpy
  ```
Vor der Ausführung der Skripte müssen die Speicherpfade angepasst werden. 
  - Skript `generate_object_centric_o2c_dataset.py`:
    
  ```
  final_excel_file = 'Pfad zum gewünschten Speicherort/ocpm-dataset.xlsx' # <-- Platzhalter für den Speicherort der Excel-Datei´
  ```
  - Skript `create_case_table.py`:

  ```
  # Laden der relevanten Tabellen
  file_path = 'Pfad zum OCPM Datensatz' # <-- Platzhalter für den Speicherort des OCPM Datensatzes
  excel_data = pd.ExcelFile(file_path)
  ```
  
   ```
  # Speichern der Case-Tabelle als Datei
  case_table.to_csv('Pfad zum gewünschten Speicherort/case-table.csv', index=False)  # <-- Platzhalter für den Speicherort der CSV-Datei
  ```
  
  - Skript `flattened_event_log.py`:
    
  ```
  # Lade den Datensatz
  new_file_path = 'Pfad zum OCPM Datensatz' # <-- Platzhalter für den Speicherort des OCPM Datensatzes
  new_excel_data = pd.ExcelFile(new_file_path)
  ```
  
  ```
  # Datei speichern (kann als CSV oder Excel exportiert werden)
  final_complete_event_log_df.to_csv('Pfad zum gewünschten Speicherort/event-log.csv', index=False)  # <-- Platzhalter für den Speicherort der CSV-Datei
  ```
