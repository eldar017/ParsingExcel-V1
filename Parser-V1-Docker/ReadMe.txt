cd to directory
docker build --network=host -t python-parser-v1 .
docker service create --name parser-v1 --mount type=bind,source=/opt/ExcelBackup,destination=/opt/ExcelBackup  python-parser-v1