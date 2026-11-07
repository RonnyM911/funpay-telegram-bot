@echo off
chcp 65001 >nul
title FunPay Bot - Stop
pushd "%~dp0"
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'bot\.py' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }"
del /q "%~dp0bot.lock" 2>nul
echo.
echo Bot stopped.
echo.
pause >nul
popd
