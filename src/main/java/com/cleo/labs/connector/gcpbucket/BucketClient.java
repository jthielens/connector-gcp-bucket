package com.cleo.labs.connector.gcpbucket;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BucketField;

public class BucketClient {
    private Storage storage;
    private Bucket bucket;

    public Bucket bucket() {
        return bucket;
    }

    public BucketClient(Storage storage, String bucket) {
        this.storage = storage;
        this.bucket = this.storage.get(bucket,
                Storage.BucketGetOption.fields(BucketField.NAME, BucketField.TIME_CREATED));
    }

    public boolean exists(Path path) {
        if (path.empty()) {
            return bucket.exists();
        } else {
            return bucket.get(path.toString(), Storage.BlobGetOption.fields(BlobField.NAME)) != null;
        }
    }

    public Blob mkdir(Path path) {
        if (path.empty()) {
            return null; // root path -- already exists
        }
        return bucket.create(path.directory(true).toString(), new byte[0], Bucket.BlobTargetOption.doesNotExist());
    }

    public boolean rmdir(Path path) {
        if (path.empty()) {
            return false; // root path -- can't remvoe it
        }
        Blob blob = bucket.get(path.directory(true).toString());
        if (blob == null) {
            return false; // didn't exist
        }
        blob.delete();
        return true;
    }

    public Blob get(Path source) {
        Blob blob = bucket.get(source.toString(),
                Storage.BlobGetOption.fields(BlobField.NAME, BlobField.SIZE, BlobField.UPDATED));
        return blob;
    }

    public boolean rename(Path source, Path target) {
        Blob blob = bucket.get(source.toString());
        if (blob == null) {
            return false; // source didn't exist
        }
        CopyWriter copyWriter = blob.copyTo(bucket.getName(), target.toString());
        Blob copied = copyWriter.getResult();
        if (copied == null) {
            return false;
        }
        return blob.delete();
    }

    public boolean delete(Path path) {
        Blob blob = bucket.get(path.toString());
        if (blob == null) {
            return false; // didn't exist
        }
        return blob.delete();
    }

    public List<Blob> list(Path path) {
        String target = path.directory(true).toString();
        Page<Blob> blobs = bucket.list(
                BlobListOption.fields(BlobField.NAME, BlobField.SIZE, BlobField.UPDATED),
                BlobListOption.prefix(target),
                BlobListOption.currentDirectory(),
                BlobListOption.pageSize(100));
        List<Blob> result = new ArrayList<>();
        for (Blob blob : blobs.iterateAll()) {
            if (!blob.getName().equals(target)) {
                result.add(blob);
            }
        }
        return result;
    }

    public Blob upload(Path path, InputStream in) {
        try {
            return bucket.create(path.toString(), in, Bucket.BlobWriteOption.doesNotExist());
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    public InputStream download(Path path) {
        return Channels.newInputStream(bucket.get(path.toString()).reader());
    }
}
