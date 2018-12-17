package com.cleo.labs.connector.gcpbucket;

import static org.junit.Assert.*;

import java.io.FileReader;
import java.util.UUID;

import org.junit.Test;

import com.cleo.connector.api.ConnectorClient;
import com.cleo.connector.api.command.ConnectorCommandResult;
import com.cleo.connector.api.command.ConnectorCommandResult.Status;
import com.cleo.connector.api.directory.Entry;
import com.cleo.labs.connector.testing.Commands;
import com.cleo.labs.connector.testing.StringCollector;
import com.cleo.labs.connector.testing.StringSource;
import com.cleo.labs.connector.testing.TestConnectorClientBuilder;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;

public class TestBucketConnectorClient {

    private ConnectorClient setup(String bucketName, boolean markDirectories) throws Exception {
        String key;
        try (FileReader jsonKey = new FileReader("/Users/jthielens/Downloads/API Project-e35caa8ee46a.json")) {
            key = CharStreams.toString(jsonKey);
        }
        TestConnectorClientBuilder builder = new TestConnectorClientBuilder(BucketConnectorSchema.class)
            .logger(System.err)
            .debug(true)
            .set("ProjectId", "602108159320")
            .set("GoogleAccountKey", key)
            .set("MarkDirectories", String.valueOf(markDirectories));
        if (!Strings.isNullOrEmpty(bucketName)) {
            builder.set("BucketName", bucketName);
        }
        return builder.build();
    }

    private static final String TEST_BUCKET = "cleo-labs-develop-1";

    @Test
    public void testRoundTrip() throws Exception {
        ConnectorClient client = setup(TEST_BUCKET, false);
        ConnectorCommandResult result;
        StringSource source = new StringSource("sample", StringSource.lorem);
        StringCollector destination = new StringCollector().name("sample");

        String testFolder = UUID.randomUUID().toString();

        // mkdir the test Folder
        result = Commands.mkdir(testFolder).go(client);
        assertEquals(Status.Success, result.getStatus());

        // check attributes (and caching)
        assertEquals(true, Commands.attr(testFolder).go(client).readAttributes().isDirectory());
        assertEquals(true, Commands.attr(testFolder+"/").go(client).readAttributes().isDirectory());

        // do a dir
        result = Commands.dir(testFolder).go(client);
        assertEquals(Status.Success, result.getStatus());
        assertTrue(result.getDirEntries().isPresent());
        assertTrue(result.getDirEntries().get().isEmpty());

        // put a file
        result = Commands.put(source, testFolder+"/sample").go(client);
        assertEquals(Status.Success, result.getStatus());

        // another dir
        result = Commands.dir(testFolder).go(client);
        assertEquals(Status.Success, result.getStatus());
        assertTrue(result.getDirEntries().isPresent());
        assertEquals(1, result.getDirEntries().get().size());
        for (Entry e : result.getDirEntries().get()) {
            assertEquals(e.isDir(), Commands.attr(e.getPath()).go(client).readAttributes().isDirectory());
        }
        String fileID = result.getDirEntries().get().get(0).getPath();

        // now get the file
        result = Commands.get(fileID, destination).go(client);
        assertEquals(Status.Success, result.getStatus());
        assertEquals(StringSource.lorem, destination.toString());

        // should still be a single file in the directory
        result = Commands.dir(testFolder).go(client);
        assertEquals(Status.Success, result.getStatus());
        assertTrue(result.getDirEntries().isPresent());
        assertEquals(1, result.getDirEntries().get().size());

        // now delete it
        result = Commands.delete(fileID).go(client);
        assertEquals(Status.Success, result.getStatus());

        // now directory should be empty again
        result = Commands.dir(testFolder).go(client);
        assertEquals(Status.Success, result.getStatus());
        assertTrue(result.getDirEntries().isPresent());
        assertTrue(result.getDirEntries().get().isEmpty());

        // now cleanup the directory
        result = Commands.rmdir(testFolder).go(client);
        assertEquals(Status.Success, result.getStatus());
    }

    @Test
    public void testRoundTripWithMarking() throws Exception {
        ConnectorClient client = setup(TEST_BUCKET, true);
        ConnectorCommandResult result;
        StringSource source = new StringSource("sample", StringSource.lorem);
        StringCollector destination = new StringCollector().name("sample");

        String testFolder = UUID.randomUUID().toString()+".dir";

        // mkdir the test Folder
        result = Commands.mkdir(testFolder).go(client);
        assertEquals(Status.Success, result.getStatus());

        // check attributes (and caching)
        assertEquals(true, Commands.attr(testFolder).go(client).readAttributes().isDirectory());
        assertEquals(true, Commands.attr(testFolder+"/").go(client).readAttributes().isDirectory());

        // do a dir
        result = Commands.dir(testFolder).go(client);
        assertEquals(Status.Success, result.getStatus());
        assertTrue(result.getDirEntries().isPresent());
        assertTrue(result.getDirEntries().get().isEmpty());

        // put a file
        result = Commands.put(source, testFolder+"/sample").go(client);
        assertEquals(Status.Success, result.getStatus());

        // another dir
        result = Commands.dir(testFolder).go(client);
        assertEquals(Status.Success, result.getStatus());
        assertTrue(result.getDirEntries().isPresent());
        assertEquals(1, result.getDirEntries().get().size());
        for (Entry e : result.getDirEntries().get()) {
            assertEquals(e.isDir(), Commands.attr(e.getPath()).go(client).readAttributes().isDirectory());
        }
        String fileID = result.getDirEntries().get().get(0).getPath();

        // now get the file
        result = Commands.get(fileID, destination).go(client);
        assertEquals(Status.Success, result.getStatus());
        assertEquals(StringSource.lorem, destination.toString());

        // should still be a single file in the directory
        result = Commands.dir(testFolder).go(client);
        assertEquals(Status.Success, result.getStatus());
        assertTrue(result.getDirEntries().isPresent());
        assertEquals(1, result.getDirEntries().get().size());

        // now delete it
        result = Commands.delete(fileID).go(client);
        assertEquals(Status.Success, result.getStatus());

        // now directory should be empty again
        result = Commands.dir(testFolder).go(client);
        assertEquals(Status.Success, result.getStatus());
        assertTrue(result.getDirEntries().isPresent());
        assertTrue(result.getDirEntries().get().isEmpty());

        // now cleanup the directory
        result = Commands.rmdir(testFolder).go(client);
        assertEquals(Status.Success, result.getStatus());
    }

    @Test
    public void testRoundTripWithMarkingFromTop() throws Exception {
        ConnectorClient clientX = setup(null, true);
        ConnectorCommandResult result;
        StringSource source = new StringSource("sample", StringSource.lorem);
        StringCollector destination = new StringCollector().name("sample");

        String testFolder = TEST_BUCKET+"/"+UUID.randomUUID().toString()+".dir";

        // mkdir the test Folder
        result = Commands.mkdir(testFolder).go(clientX);
        assertEquals(Status.Success, result.getStatus());

        // check attributes (and caching)
        assertEquals(true, Commands.attr(testFolder).go(clientX).readAttributes().isDirectory());
        assertEquals(true, Commands.attr(testFolder+"/").go(clientX).readAttributes().isDirectory());

        // do a dir
        result = Commands.dir(testFolder).go(clientX);
        assertEquals(Status.Success, result.getStatus());
        assertTrue(result.getDirEntries().isPresent());
        assertTrue(result.getDirEntries().get().isEmpty());

        // put a file
        result = Commands.put(source, testFolder+"/sample").go(clientX);
        assertEquals(Status.Success, result.getStatus());

        // another dir
        result = Commands.dir(testFolder).go(clientX);
        assertEquals(Status.Success, result.getStatus());
        assertTrue(result.getDirEntries().isPresent());
        assertEquals(1, result.getDirEntries().get().size());
        Entry e = result.getDirEntries().get().get(0);
        assertEquals(e.isDir(), Commands.attr(e.getPath()).go(clientX).readAttributes().isDirectory());
        assertEquals(testFolder.replaceFirst("\\.dir$","/sample"), e.getPath()); // .dir removed when not the last node
        String fileID = e.getPath();

        // now get the file
        result = Commands.get(fileID, destination).go(clientX);
        assertEquals(Status.Success, result.getStatus());
        assertEquals(StringSource.lorem, destination.toString());

        // should still be a single file in the directory
        result = Commands.dir(testFolder).go(clientX);
        assertEquals(Status.Success, result.getStatus());
        assertTrue(result.getDirEntries().isPresent());
        assertEquals(1, result.getDirEntries().get().size());

        // now delete it
        result = Commands.delete(fileID).go(clientX);
        assertEquals(Status.Success, result.getStatus());

        // now directory should be empty again
        result = Commands.dir(testFolder).go(clientX);
        assertEquals(Status.Success, result.getStatus());
        assertTrue(result.getDirEntries().isPresent());
        assertTrue(result.getDirEntries().get().isEmpty());

        // now cleanup the directory
        result = Commands.rmdir(testFolder).go(clientX);
        assertEquals(Status.Success, result.getStatus());
    }

    @Test
    public void testRoot() throws Exception {
        ConnectorClient client = setup(TEST_BUCKET, false);

        assertEquals(true, Commands.attr("").go(client).readAttributes().isDirectory());
        assertEquals(true, Commands.attr("").go(client).readAttributes().isDirectory());

        client = setup(null, false);

        assertEquals(true, Commands.attr("").go(client).readAttributes().isDirectory());
        assertEquals(true, Commands.attr("").go(client).readAttributes().isDirectory());
    }

}
