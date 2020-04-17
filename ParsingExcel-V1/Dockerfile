FROM python:3.6-slim-stretch
ADD ParsingExcelDocker.py /
ADD publisher.py /
RUN pip install xlrd
RUN pip install uuid
RUN pip install pika
RUN pip install openpyxl
CMD [ "python3", "./ParsingExcelDocker.py" ]
