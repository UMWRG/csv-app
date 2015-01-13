pyinstaller --upx-dir=../../upx ImportCSV.spec
xcopy dist\ImportCSV\* ..\CSVPlugin\plugins\ImportCSV\ /Y /s
copy plugin.xml ..\CSVPlugin\plugins\ImportCSV\plugin.xml
PAUSE