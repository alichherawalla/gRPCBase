// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: main/proto/sample.proto

package com.betasolutions.grpc.sample;

public interface FeatureDatabaseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:sample.FeatureDatabase)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>repeated .sample.Feature feature = 1;</code>
   */
  java.util.List<com.betasolutions.grpc.sample.Feature> 
      getFeatureList();
  /**
   * <code>repeated .sample.Feature feature = 1;</code>
   */
  com.betasolutions.grpc.sample.Feature getFeature(int index);
  /**
   * <code>repeated .sample.Feature feature = 1;</code>
   */
  int getFeatureCount();
  /**
   * <code>repeated .sample.Feature feature = 1;</code>
   */
  java.util.List<? extends com.betasolutions.grpc.sample.FeatureOrBuilder> 
      getFeatureOrBuilderList();
  /**
   * <code>repeated .sample.Feature feature = 1;</code>
   */
  com.betasolutions.grpc.sample.FeatureOrBuilder getFeatureOrBuilder(
      int index);
}