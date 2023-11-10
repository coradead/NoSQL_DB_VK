package galeev.dao;

import jdk.incubator.foreign.MemorySegment;
import galeev.dao.entry.BaseEntry;
import galeev.dao.entry.Entry;

public interface MSConverter<K, V> {

    K getKeyFromMS(MemorySegment ms);

    V getValFromMS(MemorySegment ms);

    MemorySegment getMSFromKey(K key);

    MemorySegment getMSFromVal(V val);

    default Entry<K,V> entryMStoEntry(Entry<MemorySegment, MemorySegment> entry) {
        if (entry == null) {
            return null;
        }
        return new BaseEntry<>(
                getKeyFromMS(entry.key()),
                getValFromMS(entry.value())
        );
    }

    default Entry<MemorySegment, MemorySegment> entryToEntryMS(Entry<K,V> entry) {
        return new BaseEntry<>(
                getMSFromKey(entry.key()),
                getMSFromVal(entry.value())
        );
    }
}
