package com.amazonaws.samples.offsettranslator;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Minimal filesystem abstraction for reading/writing Spark checkpoint files on local FS or S3.
 * Only the operations the translator needs: list a directory, read a blob, write a blob.
 *
 * <p>S3 paths use the form {@code s3://bucket/key/prefix}. Local paths can be absolute or relative.
 */
public interface CheckpointStore {

    /** List the immediate children of {@code dir}. Returns full paths in the same scheme as {@code dir}. */
    List<String> list(String dir) throws IOException;

    byte[] read(String path) throws IOException;

    void write(String path, byte[] bytes) throws IOException;

    static CheckpointStore forPath(String anyPath) {
        if (anyPath.startsWith("s3://")) return new S3();
        return new Local();
    }

    /** Local filesystem implementation. */
    class Local implements CheckpointStore {
        @Override public List<String> list(String dir) throws IOException {
            try (Stream<Path> s = Files.list(Paths.get(dir))) {
                return s.map(Path::toString).collect(Collectors.toList());
            }
        }
        @Override public byte[] read(String path) throws IOException { return Files.readAllBytes(Paths.get(path)); }
        @Override public void write(String path, byte[] bytes) throws IOException {
            Path p = Paths.get(path);
            if (p.getParent() != null) Files.createDirectories(p.getParent());
            Files.write(p, bytes);
        }
    }

    /** S3 implementation backed by AWS SDK v2 with default credential chain. */
    class S3 implements CheckpointStore {
        private final S3Client s3 = S3Client.create();

        @Override public List<String> list(String dir) {
            S3Uri u = S3Uri.parse(dir);
            String prefix = u.key.endsWith("/") ? u.key : u.key + "/";
            ListObjectsV2Response resp = s3.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(u.bucket).prefix(prefix).delimiter("/").build());
            List<String> out = new ArrayList<>();
            for (S3Object o : resp.contents()) out.add("s3://" + u.bucket + "/" + o.key());
            for (CommonPrefix cp : resp.commonPrefixes()) out.add("s3://" + u.bucket + "/" + cp.prefix());
            return out;
        }
        @Override public byte[] read(String path) {
            S3Uri u = S3Uri.parse(path);
            return s3.getObject(GetObjectRequest.builder().bucket(u.bucket).key(u.key).build(),
                                ResponseTransformer.toBytes()).asByteArray();
        }
        @Override public void write(String path, byte[] bytes) {
            S3Uri u = S3Uri.parse(path);
            s3.putObject(PutObjectRequest.builder().bucket(u.bucket).key(u.key).build(),
                         RequestBody.fromBytes(bytes));
        }

        private static final class S3Uri {
            final String bucket; final String key;
            S3Uri(String b, String k) { this.bucket = b; this.key = k; }
            static S3Uri parse(String s) {
                URI u = URI.create(s);
                String key = u.getPath();
                if (key.startsWith("/")) key = key.substring(1);
                return new S3Uri(u.getHost(), key);
            }
        }
    }
}
