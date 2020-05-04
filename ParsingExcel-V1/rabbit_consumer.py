from openpyxl import Workbook
import json
import pika
from openpyxl import load_workbook
import pandas as pd
import base64
import io
import openpyxl
import ExcelParsing
#import ExcelParsing
#import parser


credentials = pika.PlainCredentials(username='user', password='password')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='54.144.236.23', port='5672', credentials=credentials))
channel = connection.channel()

#channel.queue_declare(queue='hello',durable=False)
#counter = 1


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    ExcelParsing.Object.file_extension(body)
    # excel = body
    # excel_dict = json.loads(excel)
    # print (excel_dict)
    # file_extension = excel_dict["fileName"]
    # userId = excel_dict["userId"]
    # filename = excel_dict["fileName"]
    # curr_date = excel_dict["curr_date"]
    #
    # print(userId)
    # print(filename)
    # print(curr_date)

    # if str(file_extension).split(".")[1] =='xlsx':
    #     print('excel format : xlsx')
    # else:
    #     print("dddddd")
    # if str(file_extension).split(".")[1] =='xls':
    #     print('excel format : xls')
    # else:
    #     print("dddddd")
    # # if "fileName" in excel_dict:
    # #     file_extension = excel_dict["fileName"]
    # #     print(file_extension)
    # #     print(str(file_extension).split(".")[1])
    # # else:
    # #     print('STAM')
    #
    #
    # if "base64buffer" in excel_dict:
    #     encoding_data = excel_dict["base64buffer"]
    #     print(encoding_data)
    #     decoded_data = base64.b64decode(encoding_data)
    #     xls_filelike = io.BytesIO(decoded_data)
    #     workbook = openpyxl.load_workbook(xls_filelike)
    #     data = workbook
    #     #parser.read_file(data)
    #     workbook.save(filename="C:\OM\Yogev99110.xlsx")
    #
    #
    # else:
    #     print('Cannot parsing json from rabbit')


while(True):
    try:
        print("Connecting...")
        channel.basic_consume(
            queue='test', on_message_callback=callback, auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        try:
            channel.start_consuming()
            print("1 - start consuming")
        except KeyboardInterrupt:
            channel.stop_consuming()
            connection.close()
            time.sleep(5)
            print("2 - stop consuming and close connction")
            continue
    except pika.exceptions.ConnectionClosedByBroker:
        # Uncomment this to make the example not attempt recovery
        # from server-initiated connection closure, including
        # when the node is stopped cleanly
        #
        channel.start_consuming()
        print("3 - start consuming after exeption 1 closed by broker")
        continue
    # Do not recover on channel errors
    except pika.exceptions.AMQPChannelError as err:
        print("Caught a channel error: {}, stopping...".format(err))
        channel.stop_consuming()
        connection.close()
        time.sleep(5)
        print("4 - stop consuming and close connction")
        channel.start_consuming()
        print("4 - start consuming after connection closed")
        continue
    #Recover on all other connection errors
    except pika.exceptions.AMQPConnectionError:
        print("Connection was closed, retrying...")
        print("5 - retry connect")
        time.sleep(5)
        # channel.start_consuming()
        continue