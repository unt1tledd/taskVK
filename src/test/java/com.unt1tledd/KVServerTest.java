package com.unt1tledd;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.unt1tledd.controller.KVController;
import com.unt1tledd.entity.KVEntry;
import com.unt1tledd.service.KVService;
import io.grpc.*;
import org.junit.jupiter.api.*;
import api.kv.v1.Kvservice.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KVServerTest {

    private static Server grpcServer;
    private static ManagedChannel channel;
    private static KVController kvController;
    private static KVService kvServiceMock;

    @BeforeAll
    static void setupServer() throws Exception {
        kvServiceMock = mock(KVService.class);
        kvController = new KVController(kvServiceMock);

        grpcServer = ServerBuilder
                .forPort(0)
                .addService(kvController)
                .build()
                .start();

        channel = ManagedChannelBuilder
                .forAddress("localhost", grpcServer.getPort())
                .usePlaintext()
                .build();
    }

    @AfterAll
    static void teardown() throws Exception {
        if (channel != null) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (grpcServer != null) {
            grpcServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @Order(1)
    @DisplayName("PUT and GET a normal value")
    void testPutAndGetValue() {
        byte[] value = "hello".getBytes();
        when(kvServiceMock.get("my-key")).thenReturn(value);

        // PUT
        kvController.put(
                PutRequest.newBuilder()
                        .setKey("my-key")
                        .setValue(ByteString.copyFrom(value))
                        .build(),
                new TestStreamObserver<>()
        );
        verify(kvServiceMock).put("my-key", value);

        // GET
        TestStreamObserver<GetResponse> getObserver = new TestStreamObserver<>();
        kvController.get(
                GetRequest.newBuilder()
                        .setKey("my-key")
                        .build(),
                getObserver
        );

        assertThat(getObserver.received).hasSize(1);
        assertThat(getObserver.received.get(0).getValue().toByteArray()).isEqualTo(value);
    }

    @Test
    @Order(2)
    @DisplayName("PUT with null value")
    void testPutNullValue() {
        kvController.put(
                PutRequest.newBuilder()
                        .setKey("null-key")
                        .build(),
                new TestStreamObserver<>()
        );
        verify(kvServiceMock).put("null-key", null);
    }

    @Test
    @Order(3)
    @DisplayName("GET non-existing key returns notNull=false")
    void testGetNull() {
        when(kvServiceMock.get("missing")).thenReturn(null);

        TestStreamObserver<GetResponse> getObserver = new TestStreamObserver<>();
        kvController.get(
                GetRequest.newBuilder()
                        .setKey("missing")
                        .build(),
                getObserver
        );

        assertThat(getObserver.received.get(0).getNotNull()).isFalse();
    }

    @Test
    @Order(4)
    @DisplayName("RANGE returns multiple entries")
    void testRange() {
        List<KVEntry> entries = Arrays.asList(
                new KVEntry("k1", "v1".getBytes()),
                new KVEntry("k2", "v2".getBytes())
        );
        when(kvServiceMock.range("k1", "k2")).thenReturn(entries.stream());

        TestStreamObserver<RangeResponse> rangeObserver = new TestStreamObserver<>();
        kvController.range(
                RangeRequest.newBuilder()
                        .setKeySince("k1")
                        .setKeyTo("k2")
                        .build(),
                rangeObserver
        );

        assertThat(rangeObserver.received).hasSize(2);
        assertThat(rangeObserver.received.get(0).getKey()).isEqualTo("k1");
        assertThat(rangeObserver.received.get(1).getKey()).isEqualTo("k2");
    }

    @Test
    @Order(5)
    @DisplayName("COUNT returns proper value")
    void testCount() {
        when(kvServiceMock.count()).thenReturn(42L);

        TestStreamObserver<CountResponse> countObserver = new TestStreamObserver<>();
        kvController.count(Empty.getDefaultInstance(), countObserver);

        assertThat(countObserver.received.get(0).getCount().getValue()).isEqualTo(42L);
    }

    static class TestStreamObserver<T> implements io.grpc.stub.StreamObserver<T> {
        final List<T> received = new java.util.ArrayList<>();
        private Throwable error;

        @Override
        public void onNext(T value) {
            received.add(value);
        }

        @Override
        public void onError(Throwable t) {
            this.error = t;
        }

        @Override
        public void onCompleted() { }

        public Throwable getError() {
            return error;
        }
    }

    @Test
    @Order(6)
    @DisplayName("GET with empty key returns notNull=false")
    void testGetEmptyKey() {
        when(kvServiceMock.get("")).thenReturn(null);
        TestStreamObserver<GetResponse> observer = new TestStreamObserver<>();
        kvController.get(GetRequest.newBuilder().setKey("").build(), observer);
        assertFalse(observer.received.get(0).getNotNull());
    }

    @Test
    @Order(7)
    @DisplayName("PUT and GET with large value (10 MB)")
    void testPutGetLargeValue() {
        int size = 10 * 1024 * 1024;
        byte[] bigValue = new byte[size];
        Arrays.fill(bigValue, (byte) 'x');

        doNothing().when(kvServiceMock).put("big-key", bigValue);
        when(kvServiceMock.get("big-key")).thenReturn(bigValue);

        kvController.put(PutRequest.newBuilder().setKey("big-key").setValue(ByteString.copyFrom(bigValue)).build(), new TestStreamObserver<>());
        TestStreamObserver<GetResponse> getObserver = new TestStreamObserver<>();
        kvController.get(GetRequest.newBuilder().setKey("big-key").build(), getObserver);
        assertArrayEquals(bigValue, getObserver.received.get(0).getValue().toByteArray());
    }

    @Test
    @Order(8)
    @DisplayName("RANGE with invalid bounds returns empty")
    void testRangeInvalidBounds() {
        when(kvServiceMock.range("z", "a")).thenReturn(java.util.stream.Stream.empty());
        TestStreamObserver<RangeResponse> observer = new TestStreamObserver<>();
        kvController.range(RangeRequest.newBuilder().setKeySince("z").setKeyTo("a").build(), observer);
        assertThat(observer.received).isEmpty();
    }

    @Test
    @Order(9)
    @DisplayName("COUNT when empty returns 0")
    void testCountEmpty() {
        when(kvServiceMock.count()).thenReturn(0L);
        TestStreamObserver<CountResponse> observer = new TestStreamObserver<>();
        kvController.count(Empty.getDefaultInstance(), observer);
        assertEquals(0L, observer.received.get(0).getCount().getValue());
    }

//    @Test
//    @Order(10)
//    @DisplayName("Stress test: PUT, GET, RANGE, COUNT on 5_000_000 records")
//    void testStressFull() {
//        int N = 5_000_000;
//
//        doNothing().when(kvServiceMock).put(anyString(), any());
//        when(kvServiceMock.get(anyString())).thenAnswer(invocation -> {
//            String key = invocation.getArgument(0);
//            return ("value-" + key.split("-")[1]).getBytes();
//        });
//        when(kvServiceMock.range(anyString(), anyString())).thenAnswer(invocation -> {
//            int start = Integer.parseInt(invocation.getArgument(0).toString().split("-")[1]);
//            int end = Integer.parseInt(invocation.getArgument(1).toString().split("-")[1]);
//            return java.util.stream.IntStream.rangeClosed(start, end)
//                    .mapToObj(i -> new KVEntry("key-" + i, ("value-" + i).getBytes()));
//        });
//        when(kvServiceMock.count()).thenReturn((long) N);
//
//        for (int i = 0; i < N; i++) {
//            String key = "key-" + i;
//            byte[] value = ("value-" + i).getBytes();
//
//            kvController.put(
//                    PutRequest.newBuilder()
//                            .setKey(key)
//                            .setValue(ByteString.copyFrom(value))
//                            .build(),
//                    new TestStreamObserver<>()
//            );
//
//            if (i % 1_000_000 == 0 && i > 0) System.out.println("Inserted " + i + " records");
//        }
//
//        TestStreamObserver<GetResponse> getObserver = new TestStreamObserver<>();
//        kvController.get(GetRequest.newBuilder().setKey("key-123456").build(), getObserver);
//        assertThat(getObserver.received.get(0).getValue().toStringUtf8()).isEqualTo("value-123456");
//
//        TestStreamObserver<RangeResponse> rangeObserver = new TestStreamObserver<>();
//        kvController.range(RangeRequest.newBuilder().setKeySince("key-0").setKeyTo("key-4").build(), rangeObserver);
//        assertThat(rangeObserver.received).hasSize(5);
//
//        TestStreamObserver<CountResponse> countObserver = new TestStreamObserver<>();
//        kvController.count(Empty.getDefaultInstance(), countObserver);
//        assertThat(countObserver.received.get(0).getCount().getValue()).isEqualTo((long) N);
//
//        verify(kvServiceMock, times(N)).put(anyString(), any());
//    }
}