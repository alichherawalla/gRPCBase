package com.betasolutions.grpc.chat;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ChatClient {

    private final Logger logger;
    private final ManagedChannel channel;
    private final ChatGrpc.ChatStub asyncChatStub;
    private static final String CLIENT_ID = UUID.randomUUID().toString();
    private static final String USERNAME = "CHAT_CLIENT_JAVA";
    private static final List<String> TOPICS = Arrays.asList("UNIVERSAL_TOPIC", "CHAT_SERVICE_CLIENT", "new topic");

    public ChatClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
    }

    public ChatClient(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        asyncChatStub = ChatGrpc.newStub(channel);
        logger = new Logger(ChatClient.class);
    }

    public void shutdown() throws InterruptedException {
        logger.warning("Shutting down client");
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void listMessages(int currentId, int maxPageSize, String topic) {
        ListMessagesRequest request = ListMessagesRequest.newBuilder()
            .setCurrentId(currentId)
            .setMaxPageSize(maxPageSize)
            .setTopic(topic)
            .setClientId(CLIENT_ID)
            .build();

        try {
            asyncChatStub.listUserMessages(request, new StreamObserver<UserMessage>() {
                @Override
                public void onNext(UserMessage message) {
                    System.out.println("\n\n*****Message received: " + message);
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {

                }
            });

        } catch (StatusRuntimeException e) {
            logger.warning("RPC failed: {0} ", e.getStatus());
        }
    }

    public void startChatting() {
        try {
            StreamObserver<UserMessage> requestObserver = asyncChatStub.realTimeChat(new StreamObserver<UserMessage>() {
                @Override
                public void onNext(UserMessage userMessage) {
                    System.out.println("\n\n*****Message received\n" + userMessage);
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {

                }
            });

            while (true) {
                Scanner scanner = new Scanner(System.in);
                System.out.println("Please enter the topicName: ");
                String topic = scanner.nextLine();
                System.out.println("Please enter the message: ");
                String textMessage = scanner.nextLine();
                UserMessage userMessage = UserMessage.newBuilder()
                    .setTextMessage(textMessage)
                    .setTopic(topic)
                    .setUsername(USERNAME)
                    .setClientId(CLIENT_ID)
                    .build();
                requestObserver.onNext(userMessage);
            }


        } catch (StatusRuntimeException e) {
            logger.warning("RPC failed: {0}", e.getStatus());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ChatClient chatClient = new ChatClient("localhost", 8980);
        System.out.println("CLIENT_ID: " + CLIENT_ID);
        try {
            TOPICS.forEach(topic -> {
                chatClient.listMessages(0, 100, topic);
            });
            chatClient.startChatting();

        } catch (Exception e) {
            e.printStackTrace();
            new Logger(ChatClient.class).warning("shutting down {0}", (Object) e.getStackTrace());
        } finally {
            chatClient.shutdown();
        }
    }
}
