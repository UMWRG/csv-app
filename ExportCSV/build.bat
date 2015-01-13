pyinstaller --upx-dir=../../../upx ExportCSV.spec
copy dist\ExportCSV\* ..\CSVPlugin\plugins\ExportCSV\ /Y /s
copy plugin.xml ..\CSVPlugin\plugins\ExportCSV\plugin.xml
PAUSE