package com.nse.testtcpclient.marketdata;

import com.nse.testtcpclient.marketdata.protocol.FrameReader;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Accepts one connection per script, runs it, then closes the connection. */
final class FakeMitchServer implements AutoCloseable {

    interface Script {
        void run(Connection connection) throws Exception;
    }

    final class Connection {
        private final FrameReader reader;
        private final OutputStream out;

        Connection(Socket socket) throws IOException {
            this.reader = new FrameReader(socket.getInputStream());
            this.out = socket.getOutputStream();
        }

        byte[] read() throws IOException {
            byte[] frame = reader.readFrame();
            received.add(frame);
            return frame;
        }

        void write(byte[]... frames) throws IOException {
            for (byte[] frame : frames) {
                out.write(frame);
            }
            out.flush();
        }
    }

    private final ServerSocket server;
    final List<byte[]> received = new CopyOnWriteArrayList<>();
    final List<Throwable> errors = new CopyOnWriteArrayList<>();

    FakeMitchServer(Script... scripts) throws IOException {
        server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread.ofVirtual().start(() -> {
            for (Script script : scripts) {
                try (Socket socket = server.accept()) {
                    script.run(new Connection(socket));
                } catch (Exception e) {
                    if (!server.isClosed()) {
                        errors.add(e);
                    }
                }
            }
        });
    }

    int port() {
        return server.getLocalPort();
    }

    @Override
    public void close() throws IOException {
        server.close();
    }
}
