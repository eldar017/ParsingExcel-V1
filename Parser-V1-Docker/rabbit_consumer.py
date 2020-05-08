from openpyxl import Workbook
import json
import pika
from openpyxl import load_workbook
import base64
import io
import openpyxl
import ExcelParsing
import Logger
import datetime
import time
#import ExcelParsing
#import parser


credentials = pika.PlainCredentials(username='user', password='password')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='54.144.236.23', port='5672', credentials=credentials))
channel = connection.channel()

#channel.queue_declare(queue='hello',durable=False)
#counter = 1


def callback(ch, method, properties, body):
    print(" [x] Received message ")
    Logger.logger.info(' [x] Received message')
    ExcelParsing.Object.file_extension(body)
    

while(True):
    try:
        print("Connecting...")
        Logger.logger.debug('Connecting...')
        channel.basic_consume(
            queue='test', on_message_callback=callback, auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        try:
            channel.start_consuming()
            print("1 - start consuming")
            Logger.logger.debug('1 - start consuming')
        except KeyboardInterrupt:
            channel.stop_consuming()
            connection.close()
            time.sleep(5)
            print("2 - stop consuming and close connction")
            Logger.logger.debug('2 - stop consuming and close connction')
            continue
    except pika.exceptions.ConnectionClosedByBroker:
        # Uncomment this to make the example not attempt recovery
        # from server-initiated connection closure, including
        # when the node is stopped cleanly
        #
        channel.start_consuming()
        print("3 - start consuming after exeption 1 closed by broker")
        Logger.logger.debug('3 - start consuming after exeption 1 closed by broker')
        continue
    # Do not recover on channel errors
    except pika.exceptions.AMQPChannelError as err:
        print("Caught a channel error: {}, stopping...".format(err))
        Logger.logger.debug('Caught a channel error: {}, stopping...'.format(err))
        channel.stop_consuming()
        connection.close()
        time.sleep(5)
        print("4 - stop consuming and close connction")
        Logger.logger.debug('4 - stop consuming and close connction')
        channel.start_consuming()
        print("4 - start consuming after connection closed")
        Logger.logger.debug('4 - start consuming after connection closed')
        continue
    #Recover on all other connection errors
    except pika.exceptions.AMQPConnectionError:
        print("Connection was closed, retrying...")
        Logger.logger.debug('Connection was closed, retrying...')
        print("5 - retry connect")
        time.sleep(5)
        # channel.start_consuming()
        continue
