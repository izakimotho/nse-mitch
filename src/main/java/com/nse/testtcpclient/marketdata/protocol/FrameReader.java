package com.nse.testtcpclient.marketdata.protocol;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ProtocolException;

import static com.nse.testtcpclient.marketdata.protocol.MitchProtocol.LENGTH_FIELD_SIZE;

/** Reads length-prefixed frames; the u16 little-endian length includes the length field itself. */
public final class FrameReader {

    private final InputStream in;

    public FrameReader(InputStream in) {
        this.in = in;
    }

    public byte[] readFrame() throws IOException {
        byte[] header = new byte[LENGTH_FIELD_SIZE];
        readFully(header, 0, LENGTH_FIELD_SIZE);
        int length = (header[0] & 0xFF) | (header[1] & 0xFF) << 8;
        if (length < LENGTH_FIELD_SIZE) {
            throw new ProtocolException("Invalid frame length: " + length);
        }
        byte[] frame = new byte[length];
        frame[0] = header[0];
        frame[1] = header[1];
        readFully(frame, LENGTH_FIELD_SIZE, length - LENGTH_FIELD_SIZE);
        return frame;
    }

    private void readFully(byte[] target, int offset, int length) throws IOException {
        int end = offset + length;
        while (offset < end) {
            int read = in.read(target, offset, end - offset);
            if (read < 0) {
                throw new EOFException("Connection closed by peer");
            }
            offset += read;
        }
    }
}
