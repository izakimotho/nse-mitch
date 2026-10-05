package com.nse.testtcpclient.marketdata.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ProtocolException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FrameReaderTest {

    @Test
    void reassemblesFramesFromPartialReads() throws IOException {
        byte[] first = TestFrames.loginResponse('A');
        byte[] second = TestFrames.replayResponse('C');
        byte[] stream = concat(first, second);

        FrameReader reader = new FrameReader(oneByteAtATime(new ByteArrayInputStream(stream)));

        assertThat(reader.readFrame()).isEqualTo(first);
        assertThat(reader.readFrame()).isEqualTo(second);
    }

    @Test
    void rejectsLengthSmallerThanLengthField() {
        FrameReader reader = new FrameReader(new ByteArrayInputStream(new byte[]{1, 0}));

        assertThatThrownBy(reader::readFrame).isInstanceOf(ProtocolException.class);
    }

    @Test
    void eofMidFrameIsReported() {
        byte[] frame = TestFrames.loginResponse('A');
        FrameReader reader = new FrameReader(new ByteArrayInputStream(Arrays.copyOf(frame, frame.length - 1)));

        assertThatThrownBy(reader::readFrame).isInstanceOf(EOFException.class);
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] result = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    private static InputStream oneByteAtATime(InputStream in) {
        return new FilterInputStream(in) {
            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return super.read(b, off, Math.min(len, 1));
            }
        };
    }
}
