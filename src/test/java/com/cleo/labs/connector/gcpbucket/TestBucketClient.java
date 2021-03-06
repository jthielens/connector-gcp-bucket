package com.cleo.labs.connector.gcpbucket;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import com.cleo.labs.connector.common.Entry;
import com.cleo.labs.connector.common.Path;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class TestBucketClient {

    private Path parsePath(String parse) {
        return new Path()
                .delimiter(ProjectClient.SLASH)
                .suffixDirectories(true)
                .parseURIPath(parse);
    }

    private List<Entry> list(BucketClient client, String folder) {
        List<Entry> result = client.list(parsePath(folder));
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
        assertTrue(client.exists(new Path().directory(false)));
        assertTrue(client.exists(new Path().parseURIPath("folder-2").directory(true)));
        assertEquals(4, list(client, "/").size());
        assertEquals(0, list(client, "folder-2").size());
    }

}
