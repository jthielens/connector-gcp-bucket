package com.cleo.labs.connector.gcpbucket;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

import com.google.api.client.util.Strings;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BucketField;

public class BucketClient {
    private static final String DELIMITER = "/";
    private static final String DOT = ".";

    private Storage storage;
    private Bucket bucket;

    public static String normalize(String path, boolean asDirectory) {
        /*
         * Path cleanup:
         *   no nulls (assume empty)
         *   no leading / (string it)
         *   no just plain . (framework adds this to mean empty/root path)
         * For asDirectory:
         *   "" leave alone
         *   anything else needs to have / added on the end
         */
        if (path == null) {
            path = "";
        }
        if (path.startsWith(DELIMITER)) {
            path = path.substring(DELIMITER.length());
        }
        if (path.equals(DOT)) {
            path = "";
        }
        /*
         * Trailing / handling:
         *   directories end in / (except "")
         *   non-directories have trailing / removed
         */
        if (!path.isEmpty()) {
            if (asDirectory) {
                if (!path.endsWith(DELIMITER)) {
                    path += DELIMITER;
                }
            } else {
                if (path.endsWith(DELIMITER)) {
                    path = path.substring(0, path.length() - 1);
                }
            }
        }
        return path;
    }

    public BucketClient(Storage storage, String bucket) {
        this.storage = storage;
        this.bucket = this.storage.get(bucket,
                Storage.BucketGetOption.fields(BucketField.NAME, BucketField.TIME_CREATED));
    }

    public boolean exists(String path, boolean asDirectory) {
        path = normalize(path, true);
        if (Strings.isNullOrEmpty(path)) {
            return bucket.exists();
        } else {
            return bucket.get(normalize(path, asDirectory), Storage.BlobGetOption.fields(BlobField.NAME)) != null;
        }
    }

    public Blob mkdir(String path) {
        path = normalize(path, true);
        if (path.isEmpty()) {
            return null; // root path -- already exists
        }
        return bucket.create(path, new byte[0], Bucket.BlobTargetOption.doesNotExist());
    }

    public boolean rmdir(String path) {
        Blob blob = bucket.get(normalize(path, true));
        if (blob == null) {
            return false; // didn't exist
        }
        blob.delete();
        return true;
    }

    public Blob get(String source, boolean asDirectory) {
        Blob blob = bucket.get(normalize(source, asDirectory),
                Storage.BlobGetOption.fields(BlobField.NAME, BlobField.SIZE, BlobField.UPDATED));
        return blob;
    }

    public boolean rename(String source, String destination) {
        Blob blob = bucket.get(normalize(source, false));
        if (blob == null) {
            return false; // source didn't exist
        }
        return false; // don't know how to rename (yet -- could always copy/delete)
    }

    public boolean delete(String path) {
        Blob blob = bucket.get(normalize(path, false));
        if (blob == null) {
            return false; // didn't exist
        }
        blob.delete();
        return true;
    }

    public List<Blob> list(String path) {
        path = normalize(path, true);
        Page<Blob> blobs = bucket.list(
                BlobListOption.fields(BlobField.NAME, BlobField.SIZE, BlobField.UPDATED),
                BlobListOption.prefix(path),
                BlobListOption.currentDirectory(),
                BlobListOption.pageSize(100));
        List<Blob> result = new ArrayList<>();
        for (Blob blob : blobs.iterateAll()) {
            if (!blob.getName().equals(path)) {
                result.add(blob);
            }
        }
        return result;
    }

    public Blob upload(String path, InputStream in) {
        try {
            return bucket.create(normalize(path, false), in, Bucket.BlobWriteOption.doesNotExist());
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    public InputStream download(String path) {
        return Channels.newInputStream(bucket.get(normalize(path, false)).reader());
    }
}
