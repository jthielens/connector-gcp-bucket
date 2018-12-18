package com.cleo.labs.connector.gcpbucket;

import com.cleo.connector.api.ConnectorConfig;
import com.cleo.connector.api.annotations.ExcludeType;
import com.cleo.connector.api.annotations.Client;
import com.cleo.connector.api.annotations.Connector;
import com.cleo.connector.api.annotations.Info;
import com.cleo.connector.api.annotations.Property;
import com.cleo.connector.api.property.CommonProperties;
import com.cleo.connector.api.property.CommonProperty;
import com.cleo.connector.api.property.PropertyBuilder;
import com.cleo.labs.connector.common.ConfigFileImport;
import com.cleo.connector.api.interfaces.IConnectorProperty;
import com.cleo.connector.api.interfaces.IConnectorProperty.Attribute;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import static com.cleo.connector.api.property.CommonPropertyGroups.Connect;

import java.io.IOException;

@Connector(scheme = "GCPBucket", description = "GCP Bucket",
           excludeType = { @ExcludeType(type = ExcludeType.SentReceivedBoxes) })
@Client(BucketConnectorClient.class)
public class BucketConnectorSchema extends ConnectorConfig {
    @Property
    final IConnectorProperty<String> projectId = new PropertyBuilder<>("ProjectId", "")
            .setDescription("Google Project ID.")
            .setGroup(Connect)
            .build();

    @Property
    final IConnectorProperty<String> serviceAccountKey = new PropertyBuilder<>("GoogleAccountKey", "")
            .setDescription("Import Service Account Key JSON file.")
            .setGroup(Connect)
            .setExtendedClass(ConfigFileImport.class)
            .addAttribute(Attribute.Password)
            .build();

    @Property
    final IConnectorProperty<String> bucketName = new PropertyBuilder<>("BucketName", "")
            .setDescription("The name of the GCP bucket.")
            .setGroup(Connect)
            .build();

    @Property
    final IConnectorProperty<Boolean> markDirectories = new PropertyBuilder<>("MarkDirectories", Boolean.FALSE)
            .setDescription("append \".dir\" to directory names to distinguish them from object names")
            .setGroup(Connect)
            .build();

    @Property
    final IConnectorProperty<Integer> commandRetries = CommonProperties.of(CommonProperty.CommandRetries);

    @Property
    final IConnectorProperty<Integer> commandRetryDelay = CommonProperties.of(CommonProperty.CommandRetryDelay);

    @Property
    final IConnectorProperty<Boolean> doNotSendZeroLengthFiles = CommonProperties.of(CommonProperty.DoNotSendZeroLengthFiles);

    @Property
    final IConnectorProperty<Boolean> deleteReceivedZeroLengthFiles = CommonProperties.of(CommonProperty.DeleteReceivedZeroLengthFiles);

    @Property
    final IConnectorProperty<String> retrieveDirectorySort = CommonProperties.of(CommonProperty.RetrieveDirectorySort);

    @Property
    final IConnectorProperty<Boolean> enableDebug = CommonProperties.of(CommonProperty.EnableDebug);

    @Info
    protected static String info() throws IOException {
        return Resources.toString(BucketConnectorSchema.class.getResource("info.txt"), Charsets.UTF_8);
    }
}
