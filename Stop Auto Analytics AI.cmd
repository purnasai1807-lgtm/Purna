@echo off
setlocal

powershell -ExecutionPolicy Bypass -File "%~dp0scripts\stop-app.ps1"

echo.
pause
