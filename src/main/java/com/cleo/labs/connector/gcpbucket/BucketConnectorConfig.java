package com.cleo.labs.connector.gcpbucket;

import com.cleo.connector.api.property.ConnectorPropertyException;

public class BucketConnectorConfig {
    private BucketConnectorClient client;
    private BucketConnectorSchema schema;

    public BucketConnectorConfig(BucketConnectorClient client, BucketConnectorSchema schema) {
        this.client = client;
        this.schema = schema;
    }

    public String getBucketName() throws ConnectorPropertyException {
        return schema.bucketName.getValue(client);
    }

    public String getProjectId() throws ConnectorPropertyException {
        return schema.projectId.getValue(client);
    }

    public String getServiceAccountKey() throws ConnectorPropertyException {
        return ConnectorFileImport.value(schema.serviceAccountKey.getValue(client));
    }
}