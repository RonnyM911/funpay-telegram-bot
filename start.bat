@echo off
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
title FunPay - Telegram Bot
pushd "%~dp0"

echo ============================================
echo   FunPay Telegram bot. Starting...
echo ============================================

:: Если ты в РФ и пользуешься локальным TgWsProxy — он подхватится сам,
:: когда запущен. Отдельно запускать не обязательно.
python bot.py

echo.
echo Bot stopped. Press any key to exit.
pause >nul
popd
