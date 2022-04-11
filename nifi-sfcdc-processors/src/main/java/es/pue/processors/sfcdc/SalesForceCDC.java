/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.pue.processors.sfcdc;

import static org.cometd.bayeux.Channel.*;

import java.io.IOException;
import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.eclipse.jetty.util.ajax.JSON;

import es.pue.connector.BayeuxParameters;
import es.pue.connector.EmpConnector;
import es.pue.connector.LoginHelper;
import es.pue.connector.TopicSubscription;

@Tags({"SalesForce", "CDC"})
@CapabilityDescription("SalesForce CDC Processor")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="salesforce.token", description="The SalesForce token obtained from the login process")})
public class SalesForceCDC extends AbstractProcessor {

    private final ExecutorService workerThreadPool = Executors.newFixedThreadPool(1);
    private Collection<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
    private String token = null;
    private static final String APPLICATION_JSON = "application/json";
    private ObjectMapper objectMapper;

    public static final PropertyDescriptor LOGIN_URL_PROP = new PropertyDescriptor
            .Builder().name("LOGIN_URL_PROP")
            .displayName("Login URL")
            .description("SalesForce Login URL")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME_PROP = new PropertyDescriptor
            .Builder().name("USERNAME_PROP")
            .displayName("User Name")
            .description("SalesForce User Name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor PASSWORD_PROP = new PropertyDescriptor
            .Builder().name("PASSWORD_PROP")
            .displayName("Password")
            .description("SalesForce Password")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHANNEL_PROP = new PropertyDescriptor
            .Builder().name("CHANNEL_PROP")
            .displayName("Channel")
            .description("Subscription channel")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    static AllowableValue[] replayIds = new AllowableValue[]{ new AllowableValue("-2", "All Events"), new AllowableValue("-1", "Only new Events"), };
    public static final PropertyDescriptor REPLAY_ID_PROP = new PropertyDescriptor
            .Builder().name("REPLAY_ID_PROP")
            .displayName("Replay ID")
            .description("Replay ID: -2 to replay all events or -1 to replay only new events")
            .defaultValue("-2")
            .allowableValues(replayIds)
            .required(false)
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A Flowfile is routed to this relationship when everything goes well here")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A Flowfile is routed to this relationship it can not be parsed or a problem happens")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(USERNAME_PROP);
        descriptors.add(PASSWORD_PROP);
        descriptors.add(LOGIN_URL_PROP);
        descriptors.add(CHANNEL_PROP);
        descriptors.add(REPLAY_ID_PROP);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws Throwable {
        objectMapper = new ObjectMapper();
        final String userName = context.getProperty(USERNAME_PROP).getValue();
        final String password = context.getProperty(PASSWORD_PROP).getValue();
        final String loginUrl = context.getProperty(LOGIN_URL_PROP).getValue();
        String channel = context.getProperty(CHANNEL_PROP).getValue();
        BearerTokenProvider tokenProvider = new BearerTokenProvider(() -> {
            try {
                return LoginHelper.login(new URL(loginUrl), userName, password);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        BayeuxParameters params = tokenProvider.login();

        token = params.bearerToken();

        EmpConnector connector = new EmpConnector(params);
        LoggingListener loggingListener = new LoggingListener(true, true);

        connector.addListener(META_HANDSHAKE, loggingListener)
                .addListener(META_CONNECT, loggingListener)
                .addListener(META_DISCONNECT, loggingListener)
                .addListener(META_SUBSCRIBE, loggingListener)
                .addListener(META_UNSUBSCRIBE, loggingListener);

        connector.setBearerTokenProvider(tokenProvider);

        connector.start().get(5, TimeUnit.SECONDS);

        // long replayFrom = EmpConnector.REPLAY_FROM_EARLIEST;
        long replayFrom = Long.parseLong(context.getProperty(REPLAY_ID_PROP).getValue());
        TopicSubscription subscription;
        try {
            subscription = connector.subscribe(channel, replayFrom, getConsumer()).get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            // System.err.println(e.getCause().toString());
            // System.exit(1);
            throw e.getCause();
        } catch (TimeoutException e) {
            // System.err.println("Timed out subscribing");
            // System.exit(1);
            throw e.getCause();
        }

        // System.out.println(String.format("Subscribed: %s", subscription));
        getLogger().log(LogLevel.INFO, String.format("Subscribed: %s", subscription));
    }

    public Consumer<Map<String, Object>> getConsumer() {
        // return event -> workerThreadPool.submit(() -> getLogger().log(LogLevel.INFO, String.format("Received:\n%s, \nEvent processed by threadName:%s, threadId: %s", JSON.toString(event), Thread.currentThread().getName(), Thread.currentThread().getId())));
        return event -> workerThreadPool.submit(() -> events.add(event));
    }

    public String writeAsJson(Collection<Map<String, Object>> data) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(data);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (events.size() == 0) {
            return;
        }

        if ( flowFile == null ) {
            getLogger().log(LogLevel.INFO, "Creating new flowFile");
            flowFile = session.create();
            ProvenanceReporter pr = session.getProvenanceReporter();
            pr.create(flowFile);
            // return;
        }
        
        getLogger().log(LogLevel.INFO, "flowFile not null");

        try {
            // Convert to JSON String
            String json = this.writeAsJson(events);
            events.clear();

            // Output Flowfile
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream outputStream) throws IOException {
                    IOUtils.write(json, outputStream, "UTF-8");
                }
            });
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
            flowFile = session.putAttribute(flowFile, "salesforce.token", token);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (IOException e) {
            getLogger().error("Unable to process JSON ");
			session.transfer(flowFile, REL_FAILURE);
		}
    }
}