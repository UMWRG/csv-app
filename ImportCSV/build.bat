pyinstaller --upx-dir=../../../upx ImportCSV.spec
xcopy dist\ImportCSV\* ..\CSVPlugin\plugins\ImportCSV\
mkdir ..\CSVPlugin\plugins\ImportCSV\eggs\
xcopy dist\ImportCSV\eggs ..\CSVPlugin\plugins\ImportCSV\eggs /S
copy plugin.xml ..\CSVPlugin\plugins\ImportCSV\plugin.xml
PAUSE