package com.cleo.labs.connector.gcpbucket;

import java.io.IOException;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;

import com.google.cloud.storage.Bucket;

/**
 * Bucket file attribute views
 */
public class BucketRootAttributes implements DosFileAttributes, DosFileAttributeView {
    Bucket bucket;

    public BucketRootAttributes(Bucket bucket) {
        this.bucket = bucket;
    }

    @Override
    public FileTime lastModifiedTime() {
        return FileTime.from(bucket.getCreateTime(), TimeUnit.MILLISECONDS);
    }

    @Override
    public FileTime lastAccessTime() {
        return FileTime.from(bucket.getCreateTime(), TimeUnit.MILLISECONDS);
    }

    @Override
    public FileTime creationTime() {
        return FileTime.from(bucket.getCreateTime(), TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean isRegularFile() {
        return false;
    }

    @Override
    public boolean isDirectory() {
        return true;
    }

    @Override
    public boolean isSymbolicLink() {
        return false;
    }

    @Override
    public boolean isOther() {
        return false;
    }

    @Override
    public long size() {
        return 0L;
    }

    @Override
    public Object fileKey() {
        return bucket.getSelfLink();
    }

    @Override
    public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
        if (lastModifiedTime != null || lastAccessTime != null || createTime != null) {
            throw new UnsupportedOperationException("setTimes() not supported on GCP Bucket");
        }
    }

    @Override
    public String name() {
        return bucket.getName();
    }

    @Override
    public DosFileAttributes readAttributes() throws IOException {
        return this;
    }

    @Override
    public void setReadOnly(boolean value) throws IOException {
        throw new UnsupportedOperationException("setHidden() not supported on GCP Bucket");
    }

    @Override
    public void setHidden(boolean value) throws IOException {
        throw new UnsupportedOperationException("setHidden() not supported on GCP Bucket");
    }

    @Override
    public void setSystem(boolean value) throws IOException {
        throw new UnsupportedOperationException("setSystem() not supported on GCP Bucket");
    }

    @Override
    public void setArchive(boolean value) throws IOException {
        throw new UnsupportedOperationException("setArchive() not supported on GCP Bucket");
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public boolean isArchive() {
        return false;
    }

    @Override
    public boolean isSystem() {
        return false;
    }

}
