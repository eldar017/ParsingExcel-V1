import json
import pika
import Logger
import base64



rabbit_IP = '54.144.236.23'
rabbit_port= '5672'
username = 'user'
password = 'password'
queue_name = 'hello'
exchange_name = 'Eldar'
rk = 'test2'
rk_err = 'error'

def sender(message):
    credentials = pika.PlainCredentials(username=username, password=password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbit_IP, port=rabbit_port, credentials=credentials))
    channel = connection.channel()

    channel.basic_publish(exchange=exchange_name,
                          routing_key=rk,
                          body=message)
    print('successful sending message to ALGO')
    Logger.logger.info('successful sending message to ALGO')

def sender_err(message):
    #print(message)
    credentials = pika.PlainCredentials(username=username, password=password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbit_IP, port=rabbit_port, credentials=credentials))
    channel = connection.channel()

    channel.basic_publish(exchange=exchange_name,
                          routing_key=rk_err,
                          body=message)
    print('send succesfful to error queue')
    Logger.logger.info('send succesfful to error queue')
