package tech.dubs.ingest.sources;

import tech.dubs.ingest.api.Record;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileContentsSource implements Iterable<Record<byte[]>> {
    private final List<Path> filePaths = new ArrayList();

    public FileContentsSource(Path pathToFile) {
        this.filePaths.add(pathToFile);
    }

    public FileContentsSource(Path path, String glob) throws IOException {
        final PathMatcher pathMatcher = path.getFileSystem().getPathMatcher("glob:" + glob);
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                if(pathMatcher.matches(path)) {
                    filePaths.add(path);
                }

                return FileVisitResult.CONTINUE;
            }

            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public Iterator<Record<byte[]>> iterator() {
        return new Iterator<Record<byte[]>>() {
            private int index = 0;

            public Record<byte[]> next() {
                Path path = filePaths.get(this.index);

                try {
                    byte[] bytes = Files.readAllBytes(path);
                    return (new Record<>(bytes)).putMeta("path", path);
                } catch (IOException var7) {
                    throw new RuntimeException(var7);
                } finally {
                    ++this.index;
                }
            }

            public boolean hasNext() {
                return this.index < filePaths.size();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
