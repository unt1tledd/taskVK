package com.unt1tledd.controller;

import api.kv.v1.KVServiceGrpc;
import api.kv.v1.Kvservice.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Int64Value;
import com.unt1tledd.entity.KVEntry;
import com.unt1tledd.exceptions.ServiceException;
import com.unt1tledd.service.KVService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

public class KVController extends KVServiceGrpc.KVServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(KVController.class);

    private final KVService kvs;

    public KVController(KVService kvs) {
        this.kvs = kvs;
    }

    @Override
    public void put(PutRequest request, StreamObserver<Empty> response) {
        log.info("Run 'put' with key='{}'", request.getKey());
        try {
            byte[] value = request.getValue().isEmpty() ? null : request.getValue().toByteArray();
            kvs.put(request.getKey(), value);
            response.onNext(Empty.getDefaultInstance());
            response.onCompleted();
            log.info("Completed put with key='{}'", request.getKey());
        } catch (IllegalArgumentException e) {
            log.warn("Validation error in put: key='{}'", request.getKey(), e);
            response.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        } catch (ServiceException e) {
            log.error("Service error in put: key='{}'", request.getKey(), e);
            response.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        } catch (Exception e) {
            log.error("Unexpected error in put: key='{}'", request.getKey(), e);
            response.onError(Status.INTERNAL
                    .withDescription("Unexpected server error")
                    .asRuntimeException());
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> response) {
        log.info("Run 'get' with key='{}'", request.getKey());
        try {
            byte[] value = kvs.get(request.getKey());
            GetResponse.Builder respBuilder = GetResponse.newBuilder();

            if (value != null) {
                respBuilder.setValue(ByteString.copyFrom(value)).setNotNull(true);
            } else {
                respBuilder.setNotNull(false);
            }

            response.onNext(respBuilder.build());
            response.onCompleted();
            log.info("Completed 'get' with key='{}'", request.getKey());
        } catch (IllegalArgumentException e) {
            log.warn("Validation error in get: key='{}'", request.getKey(), e);
            response.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        } catch (ServiceException e) {
            log.error("Service error in get: key='{}'", request.getKey(), e);
            response.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        } catch (Exception e) {
            log.error("Unexpected error in get: key='{}'", request.getKey(), e);
            response.onError(Status.INTERNAL
                    .withDescription("Unexpected server error")
                    .asRuntimeException());
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<Empty> response) {
        log.info("Run 'delete' with key='{}'", request.getKey());
        try {
            kvs.delete(request.getKey());
            response.onNext(Empty.getDefaultInstance());
            response.onCompleted();
            log.info("Completed 'delete' with key='{}'", request.getKey());
        } catch (IllegalArgumentException e) {
            log.warn("Validation error in delete: key='{}'", request.getKey(), e);
            response.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        } catch (ServiceException e) {
            log.error("Service error in delete: key='{}'", request.getKey(), e);
            response.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        } catch (Exception e) {
            log.error("Unexpected error in delete: key='{}'", request.getKey(), e);
            response.onError(Status.INTERNAL
                    .withDescription("Unexpected server error")
                    .asRuntimeException());
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<RangeResponse> response) {
        log.info("Run 'delete' with keySince='{}' keyTo='{}'", request.getKeySince(), request.getKeyTo());
        try {
            Stream<KVEntry> kv = kvs.range(request.getKeySince(), request.getKeyTo());

            kv.forEach(el -> {
                RangeResponse.Builder builder = RangeResponse.newBuilder().setKey(el.key());
                if (el.value() != null) {
                    builder.setValue(ByteString.copyFrom(el.value()));
                }
                response.onNext(builder.build());
            });

            response.onCompleted();
            log.info("Completed 'delete' with keySince='{}' keyTo='{}'", request.getKeySince(), request.getKeyTo());
        } catch (IllegalArgumentException e) {
            log.warn("Validation error in range: keySince='{}', keyTo='{}'",
                    request.getKeySince(), request.getKeyTo(), e);
            response.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        } catch (ServiceException e) {
            log.error("Service error in range: keySince='{}', keyTo='{}'",
                    request.getKeySince(), request.getKeyTo(), e);
            response.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        } catch (Exception e) {
            log.error("Unexpected error in range: keySince='{}', keyTo='{}'",
                    request.getKeySince(), request.getKeyTo(), e);
            response.onError(Status.INTERNAL
                    .withDescription("Unexpected server error")
                    .asRuntimeException());
        }
    }

    @Override
    public void count(Empty request, StreamObserver<CountResponse> response) {
        log.info("Run 'count'");
        try {
            long count = kvs.count();
            CountResponse resp = CountResponse.newBuilder()
                    .setCount(Int64Value.of(count))
                    .build();

            response.onNext(resp);
            response.onCompleted();
            log.info("Completed 'count'");
        } catch (ServiceException e) {
            log.error("Service error in count", e);
            response.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        } catch (Exception e) {
            log.error("Unexpected error in count", e);
            response.onError(Status.INTERNAL
                    .withDescription("Unexpected server error")
                    .asRuntimeException());
        }
    }
}