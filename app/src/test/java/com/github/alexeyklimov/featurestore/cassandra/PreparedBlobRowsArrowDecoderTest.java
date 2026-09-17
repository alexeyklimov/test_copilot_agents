package com.github.alexeyklimov.featurestore.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.junit.jupiter.api.Test;

class PreparedBlobRowsArrowDecoderTest {
    @Test
    void decodesPreparedBlobOnlyRowsIntoArrowViewVectors() {
        var decoder = new PreparedBlobRowsArrowDecoder(List.of("entity", "value"));
        var frame = blobRowsFrame(
                4,
                0,
                0x0004,
                2,
                List.of(
                        List.of(bytes("entity-000000000001"), bytes("value-000000000001")),
                        java.util.Arrays.asList(null, bytes("value-000000000002"))));
        try (var allocator = new RootAllocator();
             var batch = decoder.decode(frame, allocator)) {
            assertThat(batch.root().getRowCount()).isEqualTo(2);

            var entity = (ViewVarBinaryVector) batch.root().getVector("entity");
            var value = (ViewVarBinaryVector) batch.root().getVector("value");

            assertThat(entity.get(0)).isEqualTo(bytes("entity-000000000001"));
            assertThat(entity.isNull(1)).isTrue();
            assertThat(value.get(0)).isEqualTo(bytes("value-000000000001"));
            assertThat(value.get(1)).isEqualTo(bytes("value-000000000002"));
            assertThat(entity.getDataBuffers()).hasSize(1);
            assertThat(value.getDataBuffers()).hasSize(1);
            assertThat(entity.getDataBuffers().get(0).memoryAddress()).isEqualTo(frame.memoryAddress());
            assertThat(value.getDataBuffers().get(0).memoryAddress()).isEqualTo(frame.memoryAddress());
        } finally {
            frame.release();
        }
    }

    @Test
    void retainsFrameForDecodedBatchLifetimeAndReleasesItOnClose() {
        var decoder = new PreparedBlobRowsArrowDecoder(List.of("value"));
        var frame = blobRowsFrame(4, 0, 0x0004, 1, List.of(List.of(bytes("value-000000000001"))));
        try (var allocator = new RootAllocator()) {
            assertThat(frame.refCnt()).isEqualTo(1);
            var batch = decoder.decode(frame, allocator);
            assertThat(frame.refCnt()).isEqualTo(2);
            batch.close();
            assertThat(frame.refCnt()).isEqualTo(1);
        } finally {
            frame.release();
        }
    }

    @Test
    void rejectsUnexpectedPreparedMetadataShape() {
        var decoder = new PreparedBlobRowsArrowDecoder(List.of("value"));
        try (var allocator = new RootAllocator()) {
            var metadataFrame = rawFrame(4, 0, body(buffer -> {
                buffer.writeInt(0x0002);
                buffer.writeInt(0x0000);
                buffer.writeInt(1);
            }));
            try {
                assertThatThrownBy(() -> decoder.decode(metadataFrame, allocator))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("NO_METADATA");
                assertThat(metadataFrame.refCnt()).isEqualTo(1);
            } finally {
                metadataFrame.release();
            }

            var mismatchFrame = blobRowsFrame(4, 0, 0x0004, 2, List.of(List.of(bytes("left"), bytes("right"))));
            try {
                assertThatThrownBy(() -> new PreparedBlobRowsArrowDecoder(List.of("only_value")).decode(mismatchFrame, allocator))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("column count");
                assertThat(mismatchFrame.refCnt()).isEqualTo(1);
            } finally {
                mismatchFrame.release();
            }
        }
    }

    @Test
    void rejectsUnsupportedFrameAssumptionsAndMalformedPayloadsWithoutLeakingRetains() {
        var decoder = new PreparedBlobRowsArrowDecoder(List.of("value"));
        try (var allocator = new RootAllocator()) {
            var compressedFrame = blobRowsFrame(4, 0x01, 0x0004, 1, List.of(List.of(bytes("value-000000000001"))));
            try {
                assertThatThrownBy(() -> decoder.decode(compressedFrame, allocator))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("uncompressed v4 frames");
                assertThat(compressedFrame.refCnt()).isEqualTo(1);
            } finally {
                compressedFrame.release();
            }

            var truncatedFrame = rawFrame(4, 0, body(buffer -> {
                buffer.writeInt(0x0002);
                buffer.writeInt(0x0004);
                buffer.writeInt(1);
                buffer.writeInt(1);
                buffer.writeInt(24);
                buffer.writeBytes(bytes("too-short"));
            }));
            try {
                assertThatThrownBy(() -> decoder.decode(truncatedFrame, allocator))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("blob payload overruns");
                assertThat(truncatedFrame.refCnt()).isEqualTo(1);
            } finally {
                truncatedFrame.release();
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static ByteBuf blobRowsFrame(
            int protocolVersion,
            int headerFlags,
            int metadataFlags,
            int columnCount,
            List<List<byte[]>> rows
    ) {
        return rawFrame(protocolVersion, headerFlags, body(buffer -> {
            buffer.writeInt(0x0002);
            buffer.writeInt(metadataFlags);
            buffer.writeInt(columnCount);
            buffer.writeInt(rows.size());
            for (var row : rows) {
                for (var cell : row) {
                    if (cell == null) {
                        buffer.writeInt(-1);
                    } else {
                        buffer.writeInt(cell.length);
                        buffer.writeBytes(cell);
                    }
                }
            }
        }));
    }

    private static ByteBuf rawFrame(int protocolVersion, int headerFlags, ByteBuf body) {
        var frame = Unpooled.directBuffer(9 + body.readableBytes());
        try {
            frame.writeByte(0x80 | protocolVersion);
            frame.writeByte(headerFlags);
            frame.writeShort(7);
            frame.writeByte(0x08);
            frame.writeInt(body.readableBytes());
            frame.writeBytes(body, body.readerIndex(), body.readableBytes());
            return frame;
        } finally {
            body.release();
        }
    }

    private static ByteBuf body(java.util.function.Consumer<ByteBuf> writer) {
        var body = Unpooled.directBuffer();
        writer.accept(body);
        return body;
    }
}
