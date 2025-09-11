package hu.webarticum.miniconnect.postgres.core;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import hu.webarticum.miniconnect.lang.ByteString;

public class TestMain {

    private static final int PROTO_3_0 = 196608;
    private static final int SSL_REQUEST = 0x04D2162F;
    private static final int CANCEL_REQUEST= 0x04D2162E;

    public static void main(String[] args) throws IOException {
        InetSocketAddress socketAddress = new InetSocketAddress("127.0.0.1", 15432);
        try (ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.setReuseAddress(true);
            serverSocket.bind(socketAddress);
            System.out.println("Listening on " + socketAddress + "...");

            while (true) {
                Socket socket = serverSocket.accept();
                try {
                    handleClient(socket);
                } finally {
                    socket.close();
                }
            }
        }
    }

    private static void handleClient(Socket socket) throws IOException {
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
        socket.setSoTimeout(30_000);
        System.out.println("Accepted " + socket.getRemoteSocketAddress());

        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream();
        
        int len = readI32(in);
        int code = readI32(in);
        if (code == SSL_REQUEST) {
            out.write('N');
            out.flush();
            len = readI32(in);
            code = readI32(in);
        }
        if (code == CANCEL_REQUEST) {
            readNBytes(in, len - 8);
            return;
        }
        if (code != PROTO_3_0) {
            return;
        }

        Map<String, String> params = readStartupParams(in, len - 8);
        String user = params.getOrDefault("user", "");
        String database = params.getOrDefault("database", "");
        String app = params.getOrDefault("application_name", "");
        System.out.printf("Startup: user=%s db=%s app=%s%n", user, database, app);
        
        sendAuthenticationOk(out);

        sendParameterStatus(out, "server_version", "15.0-miniconnect");
        sendParameterStatus(out, "server_encoding", "UTF8");
        sendParameterStatus(out, "client_encoding", "UTF8");
        sendParameterStatus(out, "DateStyle", "ISO, YMD");
        sendParameterStatus(out, "TimeZone", "UTC");
        sendParameterStatus(out, "standard_conforming_strings", "on");
        sendParameterStatus(out, "integer_datetimes", "on");
        if (!app.isEmpty()) {
            sendParameterStatus(out, "application_name", app);
        }
        
        Random random = new Random();
        int pid = random.nextInt();
        int secret = random.nextInt();
        sendBackendKeyData(out, pid, secret);
        sendReadyForQuery(out, (byte) 'I');
        out.flush();

        while (true) {
            int typeFlag = in.read();
            if (typeFlag < 0) {
                break;
            }
            int mlen = readI32(in);
            byte[] payload = readNBytes(in, mlen - 4);
            if (typeFlag == 'X') {
                break;
            }
            
            // TODO
            System.out.println("payload: " + ByteString.of(payload));
            
        }
    }

    private static Map<String,String> readStartupParams(InputStream in, int length) throws IOException {
        byte[] buf = readNBytes(in, length);
        Map<String,String> map = new LinkedHashMap<>();
        int i = 0;
        while (i < buf.length) {
            int k0 = i;
            while (i < buf.length && buf[i] != 0) {
                i++;
            }
            if (i == k0) {
                break;
            }
            String key = new String(buf, k0, i - k0, StandardCharsets.UTF_8);
            i++;
            int v0 = i;
            while (i < buf.length && buf[i] != 0) {
                i++;
            }
            String val = new String(buf, v0, i - v0, StandardCharsets.UTF_8);
            i++;
            map.put(key, val);
        }
        return map;
    }

    private static void sendAuthenticationOk(OutputStream out) throws IOException {
        out.write('R');
        writeI32(out, 8);
        writeI32(out, 0);
    }

    private static void sendParameterStatus(OutputStream out, String key, String value) throws IOException {
        out.write('S');
        byte[] k = (key + "\0").getBytes(StandardCharsets.UTF_8);
        byte[] v = (value + "\0").getBytes(StandardCharsets.UTF_8);
        writeI32(out, k.length + v.length + 4);
        out.write(k);
        out.write(v);
    }

    private static void sendBackendKeyData(OutputStream out, int pid, int secret) throws IOException {
        out.write('K');
        writeI32(out, 12);
        writeI32(out, pid);
        writeI32(out, secret);
    }


    private static void sendReadyForQuery(OutputStream out, byte txStatus) throws IOException {
        out.write('Z');
        writeI32(out, 5);
        out.write(txStatus);
    }

    private static int readI32(InputStream in) throws IOException {
        int b1 = in.read();
        int b2 = in.read();
        int b3 = in.read();
        int b4 = in.read();
        if ((b1 | b2 | b3 | b4) < 0) {
            throw new EOFException();
        }
        return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
    }

    private static void writeI32(OutputStream out, int value) throws IOException {
        out.write((value >>> 24) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write(value & 0xFF);
    }
    
    private static byte[] readNBytes(InputStream in, int length) throws IOException {
        byte[] result = new byte[length];
        int pos = 0;
        while (pos < length) {
            int readLength = in.read(result, pos, length - pos);
            if (readLength < 0) {
                throw new EOFException();
            }
            pos += readLength;
        }
        return result;
    }
    
}
