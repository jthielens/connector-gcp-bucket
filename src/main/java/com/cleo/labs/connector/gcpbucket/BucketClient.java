package com.cleo.labs.connector.gcpbucket;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.file.attribute.BasicFileAttributeView;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.cleo.connector.api.directory.Directory.Type;
import com.cleo.connector.api.helper.Attributes;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BucketField;

public class BucketClient extends Client {
    private Storage storage;
    private Bucket bucket;

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

    public boolean mkdir(Path path) {
        if (path.empty()) {
            return false; // root path -- already exists
        }
        bucket.create(path.directory(true).toString(), new byte[0], Bucket.BlobTargetOption.doesNotExist());
        return true;
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

    private Blob get(Path source) {
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

    private static Entry blobToEntry(Blob blob, Path path) {
        Entry entry = new Entry(path.directory() ? Type.dir : Type.file);
        entry.setDescription("GCP Storage Object");
        entry.setPathObject(path);
        if (blob.getSize() != null) {
            entry.setSize(blob.getSize());
        }
        if (blob.getUpdateTime() != null) {
            entry.setDate(Attributes.toLocalDateTime(blob.getUpdateTime()));
        }
        return entry;
    }

    public List<Entry> list(Path path) {
        String target = path.directory(true).toString();
        Page<Blob> blobs = bucket.list(
                BlobListOption.fields(BlobField.NAME, BlobField.SIZE, BlobField.UPDATED),
                BlobListOption.prefix(target),
                BlobListOption.currentDirectory(),
                BlobListOption.pageSize(100));
        List<Entry> result = new ArrayList<>();
        for (Blob blob : blobs.iterateAll()) {
            if (!blob.getName().equals(target)) {
                String name = blob.getName().substring(target.length());
                if (blob.isDirectory()) {
                    name = name.substring(0, name.length()-1); // remove trailing SLASH
                }
                Path fullPath = path.child(name).directory(blob.isDirectory()); // in this context blob.isDirectory is accurate
                Entry entry = blobToEntry(blob, fullPath);
                result.add(entry);
            }
        }
        return result;
    }

    public boolean upload(Path path, InputStream in) {
        try {
            bucket.create(path.toString(), in, Bucket.BlobWriteOption.doesNotExist());
            return true;
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

    public Optional<BasicFileAttributeView> attr(Path path) {
        if (path.empty()) {
            // return an Attr object representing the container
            return Optional.of(new BucketRootAttributes(bucket));
        } else {
            Blob blob = get(path);
            if (blob != null) {
                Entry entry = blobToEntry(blob, path);
                return Optional.of(new EntryAttributes(entry));
            }
        }
        return Optional.empty();
    }
}