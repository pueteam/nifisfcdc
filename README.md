# NiFi Salesforce CDC Processor

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

This NiFi processor connects to the Salesforce [Change Data Capture](https://developer.salesforce.com/docs/atlas.en-us.change_data_capture.meta/change_data_capture/cdc_intro.htm) to receive near-real-time changes of Salesforce records, and synchronize corresponding records in an external data store. Change Data Capture publishes change events, which represent changes to Salesforce records. Changes include creation of a new record, updates to an existing record, deletion of a record, and undeletion of a record.

## What is a Change Data Capture event?

A Change Data Capture event, or change event, is a notification that Salesforce sends when a change to a Salesforce record occurs as part of a create, update, delete, or undelete operation. The notification includes all new and changed fields, and header fields that contain information about the change. For example, header fields indicate the type of change that triggered the event and the origin of the change. Change events support all custom objects and a subset of standard objects.

## Prerequisites

- Apache Maven
- Apache NiFi
- Java 8 JDK (newer versions of the JDK may not work due to limitations with Apache NiFi)

## Building

This processor can be built by calling

```bash
mvn clean package -Dmaven.test.skip=true -DskipTests
```

in the top level directory. The resultant NAR file will be saved in ```nifi-sfcdc-nar``` directory.

## Installing

Copy the generated nar to NiFi lib directory:

```bash
cp nifi-sfcdc-nar/target/nifi-sfcdc-nar-1.0-SNAPSHOT.nar $NIFI_HOME/lib
```

You now need to restart Apache NiFi to pick up the configuration changes and new component.

```bash
$NIFI_HOME/bin/nifi.sh restart
```

If you now open NiFi in your browser, you should find that there is a Salesforce CDC processor available to you.

![Processor!](/images/processor.png "Processor")

## Using the NiFi Processor

Start by adding the Salesforce CDC processor to your workspace. Now look at the configuration and notice there are a number of options.

- **User Name**: The Salesforce user name
- **Password**: The Salesforce user password
- **Login URL**: The login url (test or production environment)
- **Channel**: The channel to listen (ie: ```/data/AssetChangeEvent```)
- Replay ID (optional): All Events or Only new Events. All Events will take all the messages available in the CDC, while Only new Events will listen for incoming messages since the processor starts.

![Configuration!](/images/configure.png "Configuration")

Now, you should be able to hook up the processor to a pipeline and get some records as soon as the are produced. Below is an example pipeline that will split the records to peform an additional call to a Salesforce endpoint to get additional info.

![Example!](/images/example.png "Example")

## Event payload example

This change event message is sent when an account record is created with a Name and Description field.

```json
{
     "data": {
         "schema": "IeRuaY6cbI_HsV8Rv1Mc5g", 
         "payload": {
             "ChangeEventHeader": {
                 "entityName": "Account", 
                 "recordIds": [
                    "<record_ID>"
                  ], 
                  "changeType": "CREATE", 
                  "changeOrigin": "com.salesforce.core", 
                  "transactionKey": "001b7375-0086-250e-e6ca-b99bc3a8b69f", 
                  "sequenceNumber": 1, 
                  "isTransactionEnd": true, 
                  "commitTimestamp": 1501010206653, 
                  "commitNumber": 92847272780, 
                  "commitUser": "<User_ID>"
             }, 
             "Name": "Acme", 
             "Description": "Worldwide leader in gadgets of the future.", 
             "OwnerId": "<Owner_ID>", 
             "CreatedDate": "2018-03-11T19:16:44Z", 
             "CreatedById": "<User_ID>", 
             "LastModifiedDate": "2018-03-11T19:16:44Z", 
             "LastModifiedById": "<User_ID>"
  }, 
  "event": {
      "replayId": 6
  }
 }, 
 "channel": "/data/ChangeEvents"
}
```

## When to use Change Data Capture

Use change events to:

- Receive notifications of Salesforce record changes, including create, update, delete, and undelete operations.
- Capture field changes for all records.
- Get broad access to all data regardless of sharing rules.
- Get information about the change in the event header, such as the origin of the change, which allows ignoring changes that your client generates.
- Perform data updates using transaction boundaries.
- Use a versioned event schema.
- Subscribe to mass changes in a scalable way.
- Get access to retained events for up to three days.