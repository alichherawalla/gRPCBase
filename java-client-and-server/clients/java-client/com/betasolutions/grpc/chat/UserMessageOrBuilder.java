// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: main/proto/chat.proto

package com.betasolutions.grpc.chat;

public interface UserMessageOrBuilder extends
    // @@protoc_insertion_point(interface_extends:chat.UserMessage)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>string username = 1;</code>
   */
  java.lang.String getUsername();
  /**
   * <code>string username = 1;</code>
   */
  com.google.protobuf.ByteString
      getUsernameBytes();

  /**
   * <code>string text_message = 2;</code>
   */
  java.lang.String getTextMessage();
  /**
   * <code>string text_message = 2;</code>
   */
  com.google.protobuf.ByteString
      getTextMessageBytes();

  /**
   * <code>int64 id = 3;</code>
   */
  long getId();

  /**
   * <code>string topic = 4;</code>
   */
  java.lang.String getTopic();
  /**
   * <code>string topic = 4;</code>
   */
  com.google.protobuf.ByteString
      getTopicBytes();

  /**
   * <code>string client_id = 5;</code>
   */
  java.lang.String getClientId();
  /**
   * <code>string client_id = 5;</code>
   */
  com.google.protobuf.ByteString
      getClientIdBytes();
}
