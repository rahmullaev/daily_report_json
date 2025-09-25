@echo off
setlocal enabledelayedexpansion

echo ========================================
echo FORCE REPLACE GIT REPO CONTENT
echo ========================================

:: =======================
:: Настройки пользователя
:: =======================
:: Путь к папке с файлами, которые нужно залить
set "SOURCE_PATH=d:\Rustam\Programming\GitActions\daily_report_json\"

:: URL удалённого репозитория GitHub (HTTPS или SSH)
set "REPO_URL=https://github.com/rahmullaev/daily_report_json"

:: Временная папка для работы
set "TEMP_REPO=%TEMP%\git_force_repo"
:: =======================

:: Удаляем временную папку, если она есть
if exist "!TEMP_REPO!" rmdir /s /q "!TEMP_REPO!"

:: Создаём временную папку
mkdir "!TEMP_REPO!"
cd /d "!TEMP_REPO!"

:: Инициализируем новый git репозиторий
git init

:: Добавляем удалённый репозиторий
git remote add origin !REPO_URL!

:: Копируем все файлы из SOURCE_PATH в TEMP_REPO
xcopy /E /H /K /Y "!SOURCE_PATH!\*" "!TEMP_REPO!\"

:: Добавляем все файлы
git add .

:: Получаем красивый timestamp
for /f "tokens=1-3 delims=/" %%a in ("%date%") do (
    set day=%%a
    set month=%%b
    set year=%%c
)
for /f "tokens=1-3 delims=:." %%a in ("%time%") do (
    set hour=%%a
    set minute=%%b
    set second=%%c
)
:: Добавляем ведущий ноль для часов
if "!hour!"==" 0" set "hour=00"
if "!hour!"==" 1" set "hour=01"
if "!hour!"==" 2" set "hour=02"
if "!hour!"==" 3" set "hour=03"
if "!hour!"==" 4" set "hour=04"
if "!hour!"==" 5" set "hour=05"
if "!hour!"==" 6" set "hour=06"
if "!hour!"==" 7" set "hour=07"
if "!hour!"==" 8" set "hour=08"
if "!hour!"==" 9" set "hour=09"

set "datetime=!year!-!month!-!day! !hour!:!minute!:!second!"
echo Committing changes with timestamp: !datetime!
git commit -m "force replace commit: !datetime!"

:: Принудительно пушим на удалённый репозиторий, полностью стирая старую историю
echo Pushing to GitHub repository (force)...
git push -u origin main --force

:: Очистка временной папки
cd /d %TEMP%
rmdir /s /q "!TEMP_REPO!"

echo.
echo ========================================
echo FORCE REPLACE COMPLETED!
echo Source folder: !SOURCE_PATH!
echo Pushed to: !REPO_URL!
echo Commit: !datetime!
echo ========================================
pause
