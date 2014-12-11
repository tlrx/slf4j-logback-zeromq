package org.tlrx.logback.appender;

import org.zeromq.ZMQ;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * @author tlrx
 * @author prixeus
 */
public class ZMQSocketOutputStream extends OutputStream {

    private final ZMQ.Socket socket;

    public ZMQSocketOutputStream(ZMQ.Socket socket) {
        this.socket = socket;
    }

    @Override
    public void write(int i) throws IOException {
        ByteBuffer b = ByteBuffer.allocate(4);
        b.putInt(i);
        socket.send(b.array(), 0);
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        socket.send(bytes, 0);
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
