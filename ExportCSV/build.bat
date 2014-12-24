pyinstaller --upx-dir=../../../upx ExportCSV.spec
copy dist\ExportCSV\* ..\CSVPlugin\plugins\ExportCSV\
mkdir ..\CSVPlugin\plugins\ExportCSV\eggs\
xcopy dist\ExportCSV\eggs ..\CSVPlugin\plugins\ExportCSV\eggs /Y
copy plugin.xml ..\CSVPlugin\plugins\ExportCSV\plugin.xml
PAUSE