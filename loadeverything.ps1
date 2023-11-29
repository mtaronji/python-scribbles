Set-location ./csv_yahoo
../.venv/scripts/python.exe loadCSVprices.py
../.venv/scripts/python.exe loadStockprices.py
../.venv/scripts/python.exe loadOptionPrices.py
pause

./exportdata.ps1
