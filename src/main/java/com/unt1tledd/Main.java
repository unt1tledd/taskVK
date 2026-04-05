package com.unt1tledd;

import com.unt1tledd.config.AppCfg;
import com.unt1tledd.controller.KVController;
import com.unt1tledd.repository.TarantoolRepository;
import com.unt1tledd.service.KVService;
import com.unt1tledd.service.KVServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        AppCfg cfg = AppCfg.load();
        try {

            TarantoolRepository trepo = new TarantoolRepository(
                    cfg.tarantoolHost(),
                    cfg.tarantoolPort(),
                    cfg.tarantoolUser(),
                    cfg.tarantoolPassword()
            );
            KVService service = new KVServiceImpl(trepo);
            Server server = ServerBuilder
                    .forPort(cfg.grpcPort())
                    .addService(new KVController(service))
                    .build();

            logger.info("Starting gRPC server on port: {}", cfg.grpcPort());
            server.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down gRPC server...");
                server.shutdown();
            }));

            server.awaitTermination();
        } catch (IOException e) {
            logger.error("CRITICAL: Failed to start gRPC server on port {}", cfg.grpcPort());
            System.exit(1);
        } catch (InterruptedException e) {
            logger.warn("WARNING: Server execution was interrupted");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("FATAL: Unexpected error during server initialization");
            System.exit(1);
        }
    }
}