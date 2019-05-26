package com.betasolutions.grpc.chat;

import io.grpc.stub.StreamObserver;

import java.util.*;

public class ChatService extends ChatGrpc.ChatImplBase {
    private final Logger logger;

    ChatService() {
        logger = new Logger(ChatService.class);
    }

    private static HashMap<String, ArrayList<UserMessage>> USER_MESSAGES = new HashMap<>();
    private static HashMap<String, HashMap<String, StreamObserver<UserMessage>>> USER_MESSAGE_OBSERVERS = new HashMap<>();

    @Override
    public StreamObserver<UserMessage> realTimeChat(StreamObserver<UserMessage> responseObserver) {
        return new StreamObserver<UserMessage>() {
            @Override
            public void onNext(UserMessage userMessage) {
                Collection<StreamObserver<UserMessage>> topicalUserMessageObservers =
                    addNewObserver(userMessage.getId(), userMessage.getTopic(), userMessage.getClientId(), responseObserver, true);
                publishMessage(userMessage, topicalUserMessageObservers);
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };
    }

    @Override
    public void listUserMessages(ListMessagesRequest request, StreamObserver<UserMessage> responseObserver) {
        ArrayList<UserMessage> userMessages = getTopicalUserMessages(request.getTopic());
        userMessages.forEach(responseObserver::onNext);
        addNewObserver(request.getCurrentId(), request.getTopic(), request.getClientId(), responseObserver, false);
    }


    private static HashMap<String, StreamObserver<UserMessage>> getTopicalUserMessageObservers(String topic) {
        if (USER_MESSAGE_OBSERVERS.get(topic) != null) {
            return USER_MESSAGE_OBSERVERS.get(topic);
        } else {
            return new HashMap<>();
        }
    }

    private ArrayList<UserMessage> getTopicalUserMessages(String topic) {
        return USER_MESSAGES.computeIfAbsent(topic, k -> new ArrayList<>());
    }

    private void publishMessage(UserMessage userMessage, Collection<StreamObserver<UserMessage>> topicalUserMessageObservers) {
        ArrayList<UserMessage> topicalUserMessages = getTopicalUserMessages(userMessage.getTopic());

        UserMessage message = UserMessage
            .newBuilder(userMessage)
            .setId(topicalUserMessages.size() + 1)
            .build();

        topicalUserMessages.add(message);

        Set<StreamObserver<UserMessage>> toRemove = new HashSet<>();
        for (StreamObserver<UserMessage> next : topicalUserMessageObservers) {
            try {
                System.out.println("publishing message received: " + message);
                next.onNext(message);
            } catch (Exception e) {

                toRemove.add(next);
                logger.info("removed");
            }
        }
        for (StreamObserver<UserMessage> next : toRemove) {
            topicalUserMessageObservers.remove(next);
        }
        USER_MESSAGES.put(userMessage.getTopic(), topicalUserMessages);
    }

    private Collection<StreamObserver<UserMessage>> addNewObserver(long currentId, String topic, String clientId,
                                                                   StreamObserver<UserMessage> responseObserver, boolean listMessages) {
        // getting all the observers for this topicName
        HashMap<String, StreamObserver<UserMessage>> observers = getTopicalUserMessageObservers(topic);
        if (observers.containsKey(clientId)) {
            return observers.values();
        } else {
            observers.put(clientId, responseObserver);
            USER_MESSAGE_OBSERVERS.put(topic, observers);
            ListMessagesRequest listMessagesRequest = ListMessagesRequest.newBuilder()
                .setClientId(clientId)
                .setCurrentId(currentId)
                .setTopic(topic)
                .setMaxPageSize(100)
                .build();
            if (listMessages) {
                listUserMessages(listMessagesRequest, responseObserver);
            }
            return observers.values();
        }
    }

}
