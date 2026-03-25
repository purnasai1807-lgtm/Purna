@echo off
setlocal

powershell -ExecutionPolicy Bypass -File "%~dp0scripts\start-app.ps1"

echo.
pause
