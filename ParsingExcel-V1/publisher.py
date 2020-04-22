import json
import pika
import base64



rabbit_IP = '54.144.236.23'
rabbit_port= '5672'
username = 'user'
password = 'password'
queue_name = 'hello'
exchange_name = 'Eldar'
rk = 'eldar'
rk_err  = 'error'

def sender(message):
    credentials = pika.PlainCredentials(username=username, password=password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbit_IP, port=rabbit_port, credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue=queue_name, durable=False)
    counter = 1


    channel.basic_publish(exchange=exchange_name,
                          routing_key=rk,
                          body=message)
    print('send succuesfuly to yoge rk')

def sender_err(message):
    credentials = pika.PlainCredentials(username=username, password=password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbit_IP, port=rabbit_port, credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue=queue_name, durable=False)
    counter = 1


    channel.basic_publish(exchange=exchange_name,
                          routing_key=rk_err,
                          body=message)
    print('send succuesfuly to error queue')