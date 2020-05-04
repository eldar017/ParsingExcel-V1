import json
import pika
from openpyxl import load_workbook
import pandas as pd
import base64
import os

class Main:

    def publishr(self):

        credentials = pika.PlainCredentials(username='user', password='password')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='54.144.236.23', port='5672', credentials=credentials))
        channel = connection.channel()

        channel.queue_declare(queue='hello', durable=False)
        with open('c:\om\Transactions_14_04_2020.xls', 'rb') as binary_file:
        #with open('c:\om\exampleVisaCard.xls', 'rb') as binary_file:
        #with open('c:\om\exampleMasterCard.xls', 'rb') as binary_file:
            binary_file_data = binary_file.read()
            base64_encoded_data = base64.b64encode(binary_file_data)
            base64_message = base64_encoded_data.decode('utf-8')
            message_format = {"userid": "omri", "time": "13/04/2020", "base64buffer": base64_message}
            message = json.dumps(message_format)

        channel.basic_publish(exchange='Eldar',
                              routing_key='test',
                              body=message)

Object = Main()
Object.publishr()