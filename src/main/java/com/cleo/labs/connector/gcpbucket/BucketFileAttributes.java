package com.cleo.labs.connector.gcpbucket;

import java.io.IOException;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.cloud.storage.Blob;

/**
 * Bucket file attribute views
 */
public class BucketFileAttributes implements DosFileAttributes, DosFileAttributeView {
    FileTime updateTime;
    FileTime createTime;
    boolean isDirectory;
    long size;
    Object selfLink;
    String name;

    public BucketFileAttributes(Blob blob, boolean isDirectory) {
        if (blob == null) {
            throw new NullPointerException("blob does not exist");
        }
        this.updateTime = FileTime.from(Optional.ofNullable(blob.getUpdateTime()).orElse(-1L), TimeUnit.MILLISECONDS);
        this.createTime = FileTime.from(Optional.ofNullable(blob.getCreateTime()).orElse(-1L), TimeUnit.MILLISECONDS);
        this.isDirectory = isDirectory;
        this.size = blob.getSize();
        this.selfLink = blob.getSelfLink();
        this.name = blob.getName();
    }

    @Override
    public FileTime lastModifiedTime() {
        return updateTime;
    }

    @Override
    public FileTime lastAccessTime() {
        return updateTime;
    }

    @Override
    public FileTime creationTime() {
        return createTime;
    }

    @Override
    public boolean isRegularFile() {
        return !isDirectory;
    }

    @Override
    public boolean isDirectory() {
        return isDirectory;
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
        return size;
    }

    @Override
    public Object fileKey() {
        return selfLink;
    }

    @Override
    public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
        if (lastModifiedTime != null || lastAccessTime != null || createTime != null) {
            throw new UnsupportedOperationException("setTimes() not supported on GCP Blobs");
        }
    }

    @Override
    public String name() {
        return name;
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
