@echo off
setlocal

powershell -ExecutionPolicy Bypass -File "%~dp0scripts\status-app.ps1"

echo.
pause
