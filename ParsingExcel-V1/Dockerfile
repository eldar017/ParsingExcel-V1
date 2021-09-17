FROM python:3.10.0rc1-slim
ADD ParsingExcelDocker.py /
ADD publisher.py /
RUN pip install xlrd
RUN pip install uuid
RUN pip install pika
RUN pip install openpyxl
CMD [ "python3", "./ParsingExcelDocker.py" ]
