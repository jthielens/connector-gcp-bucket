package com.cleo.labs.connector.gcpbucket;

import java.nio.file.attribute.BasicFileAttributeView;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.cleo.connector.api.ConnectorException;
import com.cleo.connector.api.directory.Directory.Type;
import com.cleo.connector.api.helper.Attributes;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BucketField;
import com.google.cloud.storage.Storage.BucketListOption;

public class ProjectClient extends Client {
    private Storage storage;

    public static final String SLASH = "/";  // GCP Storage uses / as the delimiter

    public ProjectClient(Storage storage) {
        this.storage = storage;
    }

    private Bucket get(Path path) {
        return storage.get(path.name(), Storage.BucketGetOption.fields(BucketField.NAME)); // path.toString() will append "/" -- use name()
    }

    @Override
    public boolean exists(Path path) {
        return path.empty() || get(path) != null;
    }

    @Override
    public boolean mkdir(Path path) throws ConnectorException {
        if (path.empty()) {
            return false; // root path -- already exists
        }
        storage.create(BucketInfo.of(path.name())); // path.toString() will append "/" -- use name()
        return true;
    }

    @Override
    public boolean rmdir(Path path) {
        if (path.empty()) {
            return false; // root path -- can't remove it
        }
        return storage.delete(path.name()); // path.toString() will append "/" -- use name()
    }

    private static Entry bucketToEntry(Bucket bucket, Path path) {
        Entry entry = new Entry(Type.dir);
        entry.setDescription("GCP Storage Account");
        entry.setPathObject(path);
        if (bucket.getCreateTime() != null) {
            entry.setDate(Attributes.toLocalDateTime(bucket.getCreateTime()));
        }
        return entry;
    }

    @Override
    public List<Entry> list(Path path) {
        Page<Bucket> buckets = storage.list(BucketListOption.fields(BucketField.NAME, BucketField.TIME_CREATED));
        List<Entry> result = new ArrayList<>();
        for (Bucket bucket : buckets.iterateAll()) {
            result.add(bucketToEntry(bucket, path.child(bucket.getName()).directory(true)));
        }
        return result;
    }

    public Optional<BasicFileAttributeView> attr(Path path) {
        if (path.empty()) {
            // return an Attr object representing the storage account
            Entry entry = new Entry(Type.dir);
            entry.setDescription("GCP Storage Account");
            entry.setPath("");
            return Optional.of(new EntryAttributes(entry));
        } else {
            Bucket bucket = get(path);
            if (bucket != null) {
                return Optional.of(new EntryAttributes(bucketToEntry(bucket, path)));
            }
        }
        return Optional.empty();
    }
}
