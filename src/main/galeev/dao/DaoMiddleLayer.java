package galeev.dao;

import galeev.dao.entry.BaseEntry;
import galeev.dao.entry.Entry;
import galeev.dao.utils.DaoConfig;
import jdk.incubator.foreign.MemorySegment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;

public class DaoMiddleLayer<K, V> {
    private final MemorySegmentDao dao;
    private final MSConverter<K, V> converter;

    public DaoMiddleLayer(DaoConfig daoConfig, MSConverter<K, V> converter) throws IOException {
        dao = new MemorySegmentDao(daoConfig);
        this.converter = converter;
    }

    public Iterator<Entry<K, V>> get(K from, @Nullable K to) {
        Iterator<Entry<MemorySegment, MemorySegment>> delegate = dao.get(converter.getMSFromKey(from), converter.getMSFromKey(to));
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public Entry<K, V> next() {
                return converter.entryMStoEntry(delegate.next());
            }
        };
    }

    public Entry<K, V> get(@Nullable K k) {
        Entry<MemorySegment, MemorySegment> entry = dao.get(converter.getMSFromKey(k));
        if (entry == null) {
            return null;
        }
        return new BaseEntry<>(
                k,
                converter.getValFromMS(entry.value())
        );
    }

    public void upsert(Entry<K, V> entry) {
        dao.upsert(converter.entryToEntryMS(entry));
    }

    public void upsert(K k, V v) {
        dao.upsert(new BaseEntry<>(
                converter.getMSFromKey(k),
                converter.getMSFromVal(v)
        ));
    }

    public void delete(K k) {
        dao.upsert(new BaseEntry<>(converter.getMSFromKey(k), null));
    }

    public void stop() throws IOException {
        dao.close();
    }
}
