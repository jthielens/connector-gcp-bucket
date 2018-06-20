package com.cleo.labs.connector.gcpbucket;

import java.io.IOException;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;

import com.google.cloud.storage.Blob;

/**
 * Bucket file attribute views
 */
public class BucketFileAttributes implements DosFileAttributes, DosFileAttributeView {
    Blob blob;

    public BucketFileAttributes(Blob blob) {
        this.blob = blob;
    }

    @Override
    public FileTime lastModifiedTime() {
        return FileTime.from(blob.getUpdateTime(), TimeUnit.MILLISECONDS);
    }

    @Override
    public FileTime lastAccessTime() {
        return FileTime.from(blob.getUpdateTime(), TimeUnit.MILLISECONDS);
    }

    @Override
    public FileTime creationTime() {
        return FileTime.from(blob.getCreateTime(), TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean isRegularFile() {
        return !blob.isDirectory();
    }

    @Override
    public boolean isDirectory() {
        return blob.isDirectory();
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
        return blob.getSize();
    }

    @Override
    public Object fileKey() {
        return blob.getSelfLink();
    }

    @Override
    public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
        if (lastModifiedTime != null || lastAccessTime != null || createTime != null) {
            throw new UnsupportedOperationException("setTimes() not supported on GCP Blobs");
        }
    }

    @Override
    public String name() {
        return blob.getName();
    }

    @Override
    public DosFileAttributes readAttributes() throws IOException {
        return this;
    }

    @Override
    public void setReadOnly(boolean value) throws IOException {
        // TODO update ACL
    }

    @Override
    public void setHidden(boolean value) throws IOException {
        throw new UnsupportedOperationException("setHidden() not supported on GCP Blobs");
    }

    @Override
    public void setSystem(boolean value) throws IOException {
        throw new UnsupportedOperationException("setSystem() not supported on GCP Blobs");
    }

    @Override
    public void setArchive(boolean value) throws IOException {
        throw new UnsupportedOperationException("setArchive() not supported on GCP Blobs");
    }

    @Override
    public boolean isReadOnly() {
        // TODO check ACL
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
