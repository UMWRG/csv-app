pyinstaller --upx-dir=../../upx ImportCSV.spec
xcopy dist\ImportCSV\* ..\CSVPlugin\plugins\ImportCSV\ /Y
mkdir ..\CSVPlugin\plugins\ImportCSV\eggs\
xcopy dist\ImportCSV\eggs ..\CSVPlugin\plugins\ImportCSV\eggs /Y
copy plugin.xml ..\CSVPlugin\plugins\ImportCSV\plugin.xml
PAUSE