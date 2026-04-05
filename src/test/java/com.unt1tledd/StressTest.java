package com.unt1tledd;

import api.kv.v1.Kvservice;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;
import api.kv.v1.KVServiceGrpc;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class StressTest {
    @Test
    void realStressTest() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9090)
                .usePlaintext()
                .build();
        KVServiceGrpc.KVServiceBlockingStub stub = KVServiceGrpc.newBlockingStub(channel);

        int N = 5_000_000;

        for (int i = 0; i < N; i++) {
            stub.put(Kvservice.PutRequest.newBuilder()
                    .setKey("key-" + i)
                    .setValue(ByteString.copyFromUtf8("value-" + i))
                    .build());
            if (i % 100_000 == 0) System.out.println("Inserted " + i);
        }

        var countResp = stub.count(Empty.getDefaultInstance());
        assertThat(countResp.getCount().getValue()).isEqualTo(N);

        channel.shutdown();
    }
}