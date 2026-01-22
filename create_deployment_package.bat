@echo off
REM Kixie Recorder Deployment Package Creator
REM This script creates a deployment package for Ubuntu servers

echo Creating Kixie Recording Uploader deployment package...

REM Create the deployment directory structure (if not exists)
if not exist "kixie-recorder-deploy" (
    echo Deployment directory not found. Please run the setup first.
    pause
    exit /b 1
)

REM Try to create tar.gz using tar if available
tar --version >nul 2>&1
if %errorlevel% equ 0 (
    echo Creating tar.gz archive with tar...
    tar -czf kixie-recorder-deploy.tar.gz kixie-recorder-deploy
    if %errorlevel% equ 0 (
        echo SUCCESS: Deployment package created as kixie-recorder-deploy.tar.gz
        goto :success
    )
)

REM Try to create zip using PowerShell
echo Creating zip archive with PowerShell...
powershell -Command "Compress-Archive -Path 'kixie-recorder-deploy' -DestinationPath 'kixie-recorder-deploy.zip' -Force"
if %errorlevel% equ 0 (
    echo SUCCESS: Deployment package created as kixie-recorder-deploy.zip
    goto :success
)

REM If neither worked, provide manual instructions
echo.
echo Unable to create archive automatically.
echo.
echo MANUAL INSTRUCTIONS:
echo ===================
echo 1. Right-click on the 'kixie-recorder-deploy' folder
echo 2. Select 'Send to' -> 'Compressed (zipped) folder'
echo 3. Rename the created zip file to 'kixie-recorder-deploy.zip'
echo 4. Upload this zip file to your Ubuntu server
echo.
echo On Ubuntu server:
echo 1. unzip kixie-recorder-deploy.zip
echo 2. cd kixie-recorder-deploy
echo 3. chmod +x setup_ubuntu.sh run_uploader.sh
echo 4. ./setup_ubuntu.sh
echo.
pause
exit /b 1

:success
echo.
echo DEPLOYMENT PACKAGE READY!
echo =========================
echo.
echo To deploy on your Ubuntu server:
echo.
echo 1. Upload the kixie-recorder-deploy.tar.gz (or .zip) file to your server
echo 2. Extract: tar -xzf kixie-recorder-deploy.tar.gz  (or unzip)
echo 3. cd kixie-recorder-deploy
echo 4. chmod +x setup_ubuntu.sh run_uploader.sh
echo 5. ./setup_ubuntu.sh
echo.
echo Then place your files:
echo - credentials.json in credentials/ folder
echo - recordings.csv in data/ folder
echo.
echo And run: ./run_uploader.sh
echo.
echo For detailed instructions, see README.md in the deployment package.
echo.
pause