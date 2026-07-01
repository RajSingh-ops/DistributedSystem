@echo off
echo ════════════════════════════════════════════════════════════
echo   DISTRIBUTED TASK EXECUTION ENGINE — CLUSTER LAUNCHER v3
echo ════════════════════════════════════════════════════════════
echo.
echo Step 1: Compiling project and generating gRPC stubs...
call .\gradlew.bat compileJava
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Build failed. See above for details.
    pause
    exit /b %ERRORLEVEL%
)
echo [OK] Build successful.
echo.
echo Step 2: Starting Orchestration Master...
start "MASTER NODE" cmd /k ".\gradlew.bat runMaster"
echo.
echo Waiting 6 seconds for Master to fully initialize...
timeout /t 6 /nobreak > nul
echo.
echo Step 3: Launching 3 Compute Worker nodes...
start "WORKER-ALPHA" cmd /k ".\gradlew.bat runWorker --args=Worker-Alpha"
start "WORKER-BETA"  cmd /k ".\gradlew.bat runWorker --args=Worker-Beta"
start "WORKER-GAMMA" cmd /k ".\gradlew.bat runWorker --args=Worker-Gamma"
echo.
echo ════════════════════════════════════════════════════════════
echo   Cluster is live!
echo   Open dashboard\index.html in your browser to monitor.
echo   Close worker windows to simulate fault tolerance.
echo ════════════════════════════════════════════════════════════
echo.
pause
