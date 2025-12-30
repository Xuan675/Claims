@echo off
cd /d %~dp0
call .venv310\Scripts\activate.bat
streamlit run app.py