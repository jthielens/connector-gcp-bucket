package com.cleo.labs.connector.gcpbucket;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import com.cleo.labs.connector.gcpbucket.BucketClient;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class TestBucketClient {

    private List<Blob> list(BucketClient client, String folder) {
        List<Blob> result = client.list(new Path().parse(folder));
        result.forEach((entry) -> System.out.println(entry));
        return result;
    }

    @Test
    @Ignore
    public void test() throws FileNotFoundException, IOException {
        String projectId = "602108159320";
        File jsonKey = new File("/Users/jthielens/Downloads/API Project-e35caa8ee46a.json");
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(jsonKey));
        Storage storage;
        storage = StorageOptions.newBuilder()
                                .setProjectId(projectId)
                                .setCredentials(credentials)
                                .build()
                                .getService();
        BucketClient client = new BucketClient(storage, "cleo-labs-develop-1");
        assertTrue(client.exists(new Path().directory(true)));
        assertTrue(client.exists(new Path().parse("folder-2").directory(true)));
        assertEquals(3, list(client, "/").size());
        assertEquals(0, list(client, "folder-2").size());
        assertTrue(client.get(new Path().parse("folder-3").directory(true)).isDirectory());
    }

}
