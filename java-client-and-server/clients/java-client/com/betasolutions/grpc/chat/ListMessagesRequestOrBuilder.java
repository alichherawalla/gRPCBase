// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: main/proto/chat.proto

package com.betasolutions.grpc.chat;

public interface ListMessagesRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:chat.ListMessagesRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>int64 current_id = 1;</code>
   */
  long getCurrentId();

  /**
   * <code>int32 max_page_size = 2;</code>
   */
  int getMaxPageSize();

  /**
   * <code>string topic = 3;</code>
   */
  java.lang.String getTopic();
  /**
   * <code>string topic = 3;</code>
   */
  com.google.protobuf.ByteString
      getTopicBytes();

  /**
   * <code>string client_id = 4;</code>
   */
  java.lang.String getClientId();
  /**
   * <code>string client_id = 4;</code>
   */
  com.google.protobuf.ByteString
      getClientIdBytes();
}
