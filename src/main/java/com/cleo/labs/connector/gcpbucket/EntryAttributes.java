package com.cleo.labs.connector.gcpbucket;

import java.io.IOException;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.ZoneOffset;
import java.util.Date;

/**
 * Bucket file attribute views
 */
public class EntryAttributes implements DosFileAttributes, DosFileAttributeView {
    Entry entry;

    public EntryAttributes(Entry entry) {
        this.entry = entry;
    }

    @Override
    public FileTime lastModifiedTime() {
        if (entry.getDate() != null) {
            return FileTime.from(entry.getDate().toInstant(ZoneOffset.UTC));
        } else {
            return FileTime.fromMillis(new Date().getTime());
        }
    }

    @Override
    public FileTime lastAccessTime() {
        throw new UnsupportedOperationException("Getting 'lastAccessTime' not supported on "+entry.getDescription());
    }

    @Override
    public FileTime creationTime() {
        throw new UnsupportedOperationException("Getting 'creationTime' not supported on "+entry.getDescription());
    }

    @Override
    public boolean isRegularFile() {
        return entry.isFile();
    }

    @Override
    public boolean isDirectory() {
        return entry.isDir();
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
        return entry.size();
    }

    @Override
    public Object fileKey() {
        throw new UnsupportedOperationException("Getting 'fileKey' not supported on "+entry.getDescription());
    }

    @Override
    public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) throws IOException {
        if (lastModifiedTime != null || lastAccessTime != null || createTime != null) {
            throw new UnsupportedOperationException("setTimes() not supported on "+entry.getDescription());
        }
    }

    @Override
    public String name() {
        return new Path().parseURIPath(entry.getPath()).name();
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
        throw new UnsupportedOperationException("setHidden() not supported on "+entry.getDescription());
    }

    @Override
    public void setSystem(boolean value) throws IOException {
        throw new UnsupportedOperationException("setSystem() not supported on "+entry.getDescription());
    }

    @Override
    public void setArchive(boolean value) throws IOException {
        throw new UnsupportedOperationException("setArchive() not supported on "+entry.getDescription());
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
