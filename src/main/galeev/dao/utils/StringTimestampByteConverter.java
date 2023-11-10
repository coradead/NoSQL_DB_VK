package galeev.dao.utils;

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import galeev.dao.MSConverter;
import galeev.dao.entry.BaseEntry;
import galeev.dao.entry.Entry;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

import static java.nio.ByteOrder.BIG_ENDIAN;

public class StringTimestampByteConverter implements MSConverter<String, Entry<Timestamp, byte[]>> {

    @Override
    public String getKeyFromMS(@Nullable MemorySegment ms) {
        return (ms == null) ? null : new String(ms.asReadOnly().toByteArray(), StandardCharsets.UTF_8);
    }

    @Override
    public Entry<Timestamp, byte[]> getValFromMS(MemorySegment inMs) {
        if (inMs == null) {
            return null;
        }
        MemorySegment ms = inMs.asReadOnly();

        Timestamp timestamp = new Timestamp(MemoryAccess.getLongAtOffset(ms,0, BIG_ENDIAN));
        int bodyLength = MemoryAccess.getIntAtOffset(ms, Long.BYTES, BIG_ENDIAN);
        if (bodyLength == -1) {
            return new BaseEntry<>(timestamp, null);
        }
        byte[] value = ms.asSlice(Long.BYTES + Integer.BYTES).toByteArray();
        return new BaseEntry<>(timestamp, value);
    }

    @Override
    public MemorySegment getMSFromKey(@Nullable String key) {
        return (key == null) ? null : MemorySegment.ofArray(key.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public MemorySegment getMSFromVal(Entry<Timestamp, byte[]> entry) {
        Timestamp timestamp = entry.key();
        byte[] body = entry.value();
        if (body == null) {
            return MemorySegment.ofByteBuffer(ByteBuffer.allocate(Long.BYTES + Integer.BYTES)
                            .putLong(timestamp.getTime())
                            .putInt(-1)
                            .flip()
                    );
        } else {
            return MemorySegment.ofByteBuffer(
                    ByteBuffer.allocate(Long.BYTES + Integer.BYTES + body.length)
                    .putLong(timestamp.getTime())
                    .putInt(body.length)
                    .put(body, 0, body.length)
                    .flip()
            );
        }
    }
}
