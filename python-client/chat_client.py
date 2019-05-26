# Copyright 2015 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The Python implementation of the gRPC route guide client."""

from __future__ import print_function

import logging
import threading
import uuid

import grpc

import chat_pb2
import chat_pb2_grpc

USERNAME = "CHAT_CLIENT_PYTHON"
TOPICS = ["UNIVERSAL_TOPIC", "CHAT_SERVICE_CLIENT", "new topic"]
MAX_PAGE_SIZE = 100
CLIENT_ID = str(uuid.uuid4())


def stream_messages(messages):
    for message in messages:
        print("\n\n******Message received \nusername: %s\ntext_message: %s \nid: %s\ntopic: %s \nclient_id:%s"
              % (message.username, message.text_message, message.id, message.topic, message.client_id))


def list_messages(stub, topic):
    list_user_message_request = chat_pb2.ListMessagesRequest(current_id=0, max_page_size=MAX_PAGE_SIZE, topic=topic,
                                                             client_id=CLIENT_ID)
    messages = stub.listUserMessages(list_user_message_request)
    stream_messages(messages)


def start_chatting(stub):
    messages = stub.realTimeChat(send_message())
    stream_messages(messages)


def send_message():
    while 1:
        topic = input("Please enter topic name")
        message = input("Please enter message")
        user_message = chat_pb2.UserMessage(text_message=message, username=USERNAME, client_id=CLIENT_ID, topic=topic)
        yield user_message


def run():
    with grpc.insecure_channel('localhost:8980') as channel:
        stub = chat_pb2_grpc.ChatStub(channel)
        threads = []
        for topic in TOPICS:
            thread = threading.Thread(target=list_messages, args=(stub, topic), daemon=True)
            thread.start()
            threads.append(thread)
        start_chatting(stub)
        for thread in threads:
            thread.join()


if __name__ == '__main__':
    logging.basicConfig()
    threading.Thread(target=run).start()
