pyinstaller -y --upx-dir=../../upx ImportCSV.spec
xcopy dist\ImportCSV\* ..\CSVPlugin\plugins\ImportCSV\ /Y /E
copy plugin.xml ..\CSVPlugin\plugins\ImportCSV\
copy icon16.png ..\CSVPlugin\plugins\ImportCSV\
copy icon32.png ..\CSVPlugin\plugins\ImportCSV\
PAUSE