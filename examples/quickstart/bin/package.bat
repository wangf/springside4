@echo off
call d:\env.bat
echo [INFO] Package the war in target dir.

cd %~dp0
cd ..
call mvn clean package -Dmaven.test.skip=true -X
cd bin
pause