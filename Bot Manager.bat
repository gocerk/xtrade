@echo off
title WEBZA Bot Manager

cls
echo.
echo.
color 0B
echo    #=======================================================#
echo    #                                                       #
echo    #   ##   ##  ####   ####   ####    ###                #
echo    #   ##   ##  #      #   #     #   #   #               #
echo    #   ## # ##  ####   ####     #    #####               #
echo    #   #######  #      #   #   #     #   #               #
echo    #    ## ##   ####   ####   ####   #   #               #
echo    #                                                       #
echo    #=======================================================#
color 07
echo.
echo        TraderPro Bot Yonetimi
echo        ---------------------
echo.
echo.
echo        1. Botu Baslat
echo        2. Botu Durdur
echo        3. Cikis
echo.

choice /C 123 /N /M "Seciminizi yapin (1-3): "

if errorlevel 3 goto end
if errorlevel 2 goto stop
if errorlevel 1 goto start

:start
cls
color 0A
echo.
echo    Bot baslatiliyor...
cd %~dp0
pm2 start ecosystem.config.js && pm2 monit
goto end

:stop
cls
color 0C
echo.
echo    Bot durduruluyor...
pm2 stop trader-bot
echo    Bot durduruldu.
goto end

:end
color 07
echo.
echo    Islem tamamlandi.
timeout /t 3
