package galeev.dao.utils;

import java.nio.file.Path;

public record DaoConfig(
        Path basePath,
        long flushThresholdBytes) {
}
