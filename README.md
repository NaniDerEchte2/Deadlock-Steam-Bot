# Deadlock Steam Bot

Ausgelagerter Steam-/Deadlock-Teil des Discord-Bots. Enthält:

- Python-Cogs unter `cogs/steam`
- Node.js Presence-Bridge unter `cogs/steam/steam_presence`

## Nutzung im Hauptbot

1. Repo nach `C:\Users\Nani-Admin\Documents\Deadlock-Steam-Bot` klonen (Standard-Pfad).
2. Im Hauptbot wird dieser Pfad automatisch als zusätzliche Cogs-Quelle erkannt.
   Alternativ den Pfad via Umgebungsvariable setzen:

   ```powershell
   $env:STEAM_COGS_DIR = "C:\\Users\\Nani-Admin\\Documents\\Deadlock-Steam-Bot\\cogs"
   ```

3. Node-Dependencies installieren (falls benötigt):

   ```powershell
   cd cogs/steam/steam_presence
   npm install
   ```

Secrets/Token gehören **nicht** ins Repo. Der Ordner `.steam-data` ist absichtlich ignoriert.
