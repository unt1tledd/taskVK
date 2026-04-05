package com.unt1tledd.config;

public record AppCfg(
        int grpcPort,
        String tarantoolHost,
        int tarantoolPort,
        String tarantoolUser,
        String tarantoolPassword
) {
    public static AppCfg load() {
        String host = System.getenv().getOrDefault("TARANTOOL_HOST", "localhost");

        int port = getEnvInt("TARANTOOL_PORT", 3301);
        int grpc = getEnvInt("GRPC_PORT", 9090);
        String user = System.getenv().getOrDefault("TARANTOOL_USER", "kvadmin");
        String pass = System.getenv().getOrDefault("TARANTOOL_PASS", "kvpassword");

        return new AppCfg(grpc, host, port, user, pass);
    }

    private static int getEnvInt(String key, int defaultValue) {
        String val = System.getenv(key);
        try {
            return (val != null) ? Integer.parseInt(val) : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}