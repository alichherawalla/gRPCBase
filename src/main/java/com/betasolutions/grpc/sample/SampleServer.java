/*
 * Copyright 2015 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.betasolutions.grpc.sample;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Math.*;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A sample gRPC server that serve the RouteGuide (see route_guide.proto) service.
 */
public class SampleServer {
    private static final Logger logger = Logger.getLogger(SampleServer.class.getName());

    private final int port;
    private final Server server;

    public SampleServer(int port) throws IOException {
        this(port, SampleUtil.getDefaultFeaturesFile());
    }

    /**
     * Create a RouteGuide server listening on {@code port} using {@code featureFile} database.
     */
    public SampleServer(int port, URL featureFile) throws IOException {
        this(ServerBuilder.forPort(port), port, SampleUtil.parseFeatures(featureFile));
    }

    /**
     * Create a RouteGuide server using serverBuilder as a base and features as data.
     */
    public SampleServer(ServerBuilder<?> serverBuilder, int port, Collection<Feature> features) {
        this.port = port;
        server = serverBuilder
            .addService(new UserMessagingService())
            .addService(new RouteGuideService(features))
            .build();
    }

    /**
     * Start serving requests.
     */
    public void start() throws IOException {
        server.start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may has been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                SampleServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    /**
     * Stop serving requests and shutdown resources.
     */
    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main method.  This comment makes the linter happy.
     */
    public static void main(String[] args) throws Exception {
        SampleServer server = new SampleServer(8980);
        server.start();
        server.blockUntilShutdown();
    }

    private static class UserMessagingService extends MessagingGrpc.MessagingImplBase {

        static HashMap<String, ArrayList<UserMessage>> USER_MESSAGES = new HashMap<>();
        static HashMap<String, Set<StreamObserver<UserMessage>>> USER_MESSAGE_OBSERVERS = new HashMap<>();

        UserMessagingService() {

        }

        @Override
        public void sendUserMessage(UserMessage request, StreamObserver<UserMessage> responseObserver) {
            ArrayList<UserMessage> topicalUserMessages = getTopicalUserMessages(request.getTopic());
            UserMessage userMessage = UserMessage.newBuilder()
                .setUsername(request.getUsername())
                .setTextMessage(request.getTextMessage())
                .setTopic(request.getTopic())
                .setId(topicalUserMessages.size() + 1)
                .build();
            topicalUserMessages.add(userMessage);
            Set<StreamObserver<UserMessage>> topicalUserMessageObservers = getTopicalUserMessageObservers(userMessage.getTopic());
            Set<StreamObserver<UserMessage>> toRemove = new HashSet<>();
            for (StreamObserver<UserMessage> next : topicalUserMessageObservers) {
                try {
                    next.onNext(userMessage);
                } catch (Exception e) {

                    toRemove.add(next);
                    logger.info("removed");
                }
            }
            for (StreamObserver<UserMessage> next : toRemove) {
                topicalUserMessages.remove(next);
            }
            USER_MESSAGES.put(request.getTopic(), topicalUserMessages);
            responseObserver.onNext(request);
            responseObserver.onCompleted();

        }

        @Override
        public void listUserMessages(ListMessagesRequest request, StreamObserver<UserMessage> responseObserver) {
            String topic = request.getTopic();
            Set<StreamObserver<UserMessage>> topicalUserMessageObservers = getTopicalUserMessageObservers(request.getTopic());
            topicalUserMessageObservers.add(responseObserver);
            USER_MESSAGE_OBSERVERS.put(topic, topicalUserMessageObservers);
            ArrayList<UserMessage> topicalUserMessages = getTopicalUserMessages(request.getTopic());
            long nextId = request.getCurrentId();
            long size = Math.max(topicalUserMessages.size(), 0);
            long startIndex = Math.min(size, Math.max(size - request.getMaxPageSize(), nextId));
            logger.info("startIndex = " + startIndex + " size: " + size + " currentId: " + nextId);
            if (size == 0 || nextId > size) {
                responseObserver.onNext(null);
            } else {
                for (UserMessage userMessage : topicalUserMessages.subList((int) startIndex, (int) size)) {
                    responseObserver.onNext(userMessage);
                }
            }
        }

        private static ArrayList<UserMessage> getTopicalUserMessages(String topic) {
            if (USER_MESSAGES.get(topic) != null) {
                return USER_MESSAGES.get(topic);
            } else {
                return new ArrayList<>();
            }
        }

        private static Set<StreamObserver<UserMessage>> getTopicalUserMessageObservers(String topic) {
            if (USER_MESSAGE_OBSERVERS.get(topic) != null) {
                return USER_MESSAGE_OBSERVERS.get(topic);
            } else {
                return new HashSet<>();
            }
        }
    }

    /**
     * Our implementation of RouteGuide service.
     *
     * <p>See route_guide.proto for details of the methods.
     */
    private static class RouteGuideService extends SampleGrpc.SampleImplBase {
        private final Collection<Feature> features;
        private final ConcurrentMap<Point, List<RouteNote>> routeNotes =
            new ConcurrentHashMap<Point, List<RouteNote>>();

        RouteGuideService(Collection<Feature> features) {
            this.features = features;
        }

        /**
         * Gets the {@link Feature} at the requested {@link Point}. If no feature at that location
         * exists, an unnamed feature is returned at the provided location.
         *
         * @param request          the requested location for the feature.
         * @param responseObserver the observer that will receive the feature at the requested point.
         */
        @Override
        public void getFeature(Point request, StreamObserver<Feature> responseObserver) {
            logger.log(Level.INFO, "getFeature Incoming request");
            responseObserver.onNext(checkFeature(request));
            responseObserver.onCompleted();
        }

        /**
         * Gets all features contained within the given bounding {@link Rectangle}.
         *
         * @param request          the bounding rectangle for the requested features.
         * @param responseObserver the observer that will receive the features.
         */
        @Override
        public void listFeatures(Rectangle request, StreamObserver<Feature> responseObserver) {
            int left = min(request.getLo().getLongitude(), request.getHi().getLongitude());
            int right = max(request.getLo().getLongitude(), request.getHi().getLongitude());
            int top = max(request.getLo().getLatitude(), request.getHi().getLatitude());
            int bottom = min(request.getLo().getLatitude(), request.getHi().getLatitude());

            for (Feature feature : features) {
                if (!SampleUtil.exists(feature)) {
                    continue;
                }

                int lat = feature.getLocation().getLatitude();
                int lon = feature.getLocation().getLongitude();
                if (lon >= left && lon <= right && lat >= bottom && lat <= top) {
                    logger.log(Level.INFO, "listFeatures on Next");
                    responseObserver.onNext(feature);
                }
            }
            responseObserver.onCompleted();
        }

        /**
         * Gets a stream of points, and responds with statistics about the "trip": number of points,
         * number of known features visited, total distance traveled, and total time spent.
         *
         * @param responseObserver an observer to receive the response summary.
         * @return an observer to receive the requested route points.
         */
        @Override
        public StreamObserver<Point> recordRoute(final StreamObserver<RouteSummary> responseObserver) {
            return new StreamObserver<Point>() {
                int pointCount;
                int featureCount;
                int distance;
                Point previous;
                final long startTime = System.nanoTime();

                @Override
                public void onNext(Point point) {
                    pointCount++;
                    if (SampleUtil.exists(checkFeature(point))) {
                        featureCount++;
                    }
                    // For each point after the first, add the incremental distance from the previous point to
                    // the total distance value.
                    if (previous != null) {
                        distance += calcDistance(previous, point);
                    }
                    previous = point;
                }

                @Override
                public void onError(Throwable t) {
                    logger.log(Level.WARNING, "recordRoute cancelled");
                }

                @Override
                public void onCompleted() {
                    long seconds = NANOSECONDS.toSeconds(System.nanoTime() - startTime);
                    responseObserver.onNext(RouteSummary.newBuilder().setPointCount(pointCount)
                        .setFeatureCount(featureCount).setDistance(distance)
                        .setElapsedTime((int) seconds).build());
                    responseObserver.onCompleted();
                }
            };
        }

        /**
         * Receives a stream of message/location pairs, and responds with a stream of all previous
         * messages at each of those locations.
         *
         * @param responseObserver an observer to receive the stream of previous messages.
         * @return an observer to handle requested message/location pairs.
         */
        @Override
        public StreamObserver<RouteNote> routeChat(final StreamObserver<RouteNote> responseObserver) {
            return new StreamObserver<RouteNote>() {
                @Override
                public void onNext(RouteNote note) {
                    List<RouteNote> notes = getOrCreateNotes(note.getLocation());

                    // Respond with all previous notes at this location.
                    for (RouteNote prevNote : notes.toArray(new RouteNote[0])) {
                        responseObserver.onNext(prevNote);
                    }

                    // Now add the new note to the list
                    notes.add(note);
                }

                @Override
                public void onError(Throwable t) {
                    logger.log(Level.WARNING, "routeChat cancelled");
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }

        /**
         * Get the notes list for the given location. If missing, create it.
         */
        private List<RouteNote> getOrCreateNotes(Point location) {
            List<RouteNote> notes = Collections.synchronizedList(new ArrayList<RouteNote>());
            List<RouteNote> prevNotes = routeNotes.putIfAbsent(location, notes);
            return prevNotes != null ? prevNotes : notes;
        }

        /**
         * Gets the feature at the given point.
         *
         * @param location the location to check.
         * @return The feature object at the point. Note that an empty name indicates no feature.
         */
        private Feature checkFeature(Point location) {
            for (Feature feature : features) {
                if (feature.getLocation().getLatitude() == location.getLatitude()
                    && feature.getLocation().getLongitude() == location.getLongitude()) {
                    return feature;
                }
            }

            // No feature was found, return an unnamed feature.
            return Feature.newBuilder().setName("").setLocation(location).build();
        }

        /**
         * Calculate the distance between two points using the "haversine" formula.
         * The formula is based on http://mathforum.org/library/drmath/view/51879.html.
         *
         * @param start The starting point
         * @param end   The end point
         * @return The distance between the points in meters
         */
        private static int calcDistance(Point start, Point end) {
            int r = 6371000; // earth radius in meters
            double lat1 = toRadians(SampleUtil.getLatitude(start));
            double lat2 = toRadians(SampleUtil.getLatitude(end));
            double lon1 = toRadians(SampleUtil.getLongitude(start));
            double lon2 = toRadians(SampleUtil.getLongitude(end));
            double deltaLat = lat2 - lat1;
            double deltaLon = lon2 - lon1;

            double a = sin(deltaLat / 2) * sin(deltaLat / 2)
                + cos(lat1) * cos(lat2) * sin(deltaLon / 2) * sin(deltaLon / 2);
            double c = 2 * atan2(sqrt(a), sqrt(1 - a));

            return (int) (r * c);
        }
    }
}