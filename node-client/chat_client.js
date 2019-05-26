var chat = require('./grpc-chat-client/chat_pb');
var services = require('./grpc-chat-client/chat_grpc_pb');

var async = require('async');
var fs = require('fs');
var parseArgs = require('minimist');
var path = require('path');
var uuid = require('uuid/v1');
var _ = require('lodash');
var grpc = require('grpc');
const inquirer = require('inquirer');

var client = new services.ChatClient('localhost:8980', grpc.credentials.createInsecure());

const CLIENT_ID = uuid();
const USERNAME = "CHAT_CLIENT_NODE";

const questions = [
  {
    type: 'input',
    name: 'topic',
    message: "Enter the topic?",
  },
  {
    type: 'input',
    name: 'message',
    message: "Enter the message?",
  }
];


function listMessages(callback) {
  const currentId = 0;
  const maxPageSize = 100;
  const topic = 'new topic';
  const listMessagesRequest = new chat.ListMessagesRequest();
  listMessagesRequest.setClientId(CLIENT_ID);
  listMessagesRequest.setCurrentId(currentId);
  listMessagesRequest.setMaxPageSize(maxPageSize);
  listMessagesRequest.setTopic(topic);
  const messageStream = client.listUserMessages(listMessagesRequest);
  messageStream.on('data', (userMessage) => {
    printMessage(userMessage)
  });
  messageStream.on('end', (evt) => {
    console.log({evt})
  });
};

function startChatting(callback) {
  console.log(callback);

  const messageStream = client.realTimeChat();
  messageStream.on('data', (userMessage) => {
    printMessage(userMessage)
  });

  messageStream.on('end', (evt) => {
    console.log({evt})
  });
  sendMessage(messageStream)
};

function sendMessage(messageStream) {
  inquirer.prompt(questions).then(answers => {
    const userMessage = new chat.UserMessage();
    userMessage.setTextMessage(answers['message']);
    userMessage.setTopic(answers['topic']);
    userMessage.setUsername(USERNAME);
    userMessage.setClientId(CLIENT_ID);
    messageStream.write(userMessage);
    sendMessage(messageStream)
  })
}

function printMessage(userMessage) {
  console.log('\n\n******Message received: username\n', userMessage.getUsername() +
    '\ntext_message: ' + userMessage.getTextMessage() +
    '\nid: ' + userMessage.getId() +
    '\ntopic: ' + userMessage.getTopic() +
    '\nclient_id: ' + userMessage.getClientId())
}

/**
 * Run all of the demos in order
 */
function main() {
  async.series([listMessages]);
  async.series([startChatting])
}

if (require.main === module) {
  main();
}

exports.listMessages = listMessages;

exports.startChatting = startChatting;
