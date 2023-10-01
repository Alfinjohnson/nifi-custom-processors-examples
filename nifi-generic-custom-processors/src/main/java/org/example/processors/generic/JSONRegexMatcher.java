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
package org.example.processors.generic;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

import static org.example.processors.generic.util.Constant.*;
import static org.example.processors.generic.util.Helper.applyRegexPattern;
import static org.example.processors.generic.util.Helper.flowFileReader;


@Tags({"json,regex,replacer"})
@CapabilityDescription("Replace specified json key-value using regex pattern")
@EventDriven
@SupportsBatching
@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class JSONRegexMatcher extends AbstractProcessor {

    /* property builder function  */
    static final PropertyDescriptor REGEX;
    static final PropertyDescriptor JSON_FIELD;
    private static String regexPattern;
    private static String keyToMatch;


    /* custom relationship declaration and assignment*/
    static final Relationship REL_SUCCESS = new Relationship.Builder().name(REL_SUCCESS_NAME)
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name(REL_FAILED_NAME).build();


    static {
        PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder();
        builder.name(JSONRegexMatcher_REGEX_NAME);
        builder.displayName(JSONRegexMatcher_REGEX_NAME);
        builder.description(JSONRegexMatcher_REGEX_NAMES_DESCRIPTION);
        builder.required(true);
        builder.defaultValue(JSONRegexMatcher_REGEX_NAMES_PROPERTY_EXAMPLE);
        builder.addValidator(StandardValidators.NON_EMPTY_VALIDATOR);
        REGEX = builder
                .build();
    }

    static {
        PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder();
        builder.name(JSONRegexMatcher_FIELD_NAME);
        builder.displayName(JSONRegexMatcher_FIELD_NAME);
        builder.description(JSONRegexMatcher_FIELD_DESCRIPTION);
        builder.defaultValue(JSONRegexMatcher_FIELD_PROPERTY_EXAMPLE);
        builder.required(true);
        builder.addValidator(StandardValidators.NON_EMPTY_VALIDATOR);
        JSON_FIELD = builder
                .build();
    }
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;


    /* processor initialization method */
    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();

        /* intRelationships and propertyDescriptors  ProcessorInitialization */
        propertyDescriptors.add(JSON_FIELD);
        propertyDescriptors.add(REGEX);
        this.descriptors = Collections.unmodifiableList(propertyDescriptors);

        final Set<Relationship> intRelationships = new HashSet<>();
        intRelationships.add(REL_FAILURE);
        intRelationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(intRelationships);
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
    public void onScheduled(final @NotNull ProcessContext context) {
        // loading values from property files
        regexPattern = context.getProperty(REGEX).getValue();
        keyToMatch = context.getProperty(JSON_FIELD).getValue();
        getLogger().info("input values : regex Pattern {}, key To Match {} ", regexPattern,keyToMatch );

    }
    /**
     * @param context
     * @param session
     */
    @Override
    public void onTrigger(final ProcessContext context, final @NotNull ProcessSession session) throws NullPointerException {
        getLogger().info("inside onTrigger method");
        /* Getting flow file from session */
        FlowFile inputFlowFile = session.get();

        if (inputFlowFile == null) {
            getLogger().info("input FlowFile null");
            session.transfer((FlowFile) null, REL_FAILURE);
            return;
        }
        if (inputFlowFile.getSize() == 0) {
            getLogger().error("Empty measurements");
            session.transfer((FlowFile) null, REL_FAILURE);
            return;
        }

        try {
            getLogger().info("reading json file from inputFlowFile");
            String jsonContent = flowFileReader(inputFlowFile, session);
            getLogger().info("jsonContent {}", jsonContent);

            Gson gson = new Gson();
            JsonObject jsonObject = gson.fromJson(jsonContent, JsonObject.class);
            if (jsonObject.has(keyToMatch)) {
                String originalValue = jsonObject.get(keyToMatch).getAsString();
                String newValue = applyRegexPattern(originalValue, regexPattern);

                jsonObject.addProperty(keyToMatch, newValue);
                getLogger().info("jsonObject addProperty {}",jsonObject);
            } else {
                getLogger().info("Key not found in JSON object.");
            }

            String updatedJsonString = new Gson().toJson(jsonObject);
            getLogger().info("updatedJsonString {}",updatedJsonString);

            getLogger().info("sessionTransfer .");
            sessionTransfer(inputFlowFile, session, updatedJsonString);
        }catch ( NullPointerException ex){
            getLogger().info("Ran into Error while generating the json {}", ex);
            session.transfer(inputFlowFile, REL_FAILURE);
        }

    }

    private void sessionTransfer(FlowFile inputFlowFile, @NotNull ProcessSession session, String jsonContent) {
        /*  Creating and writing session for REL_SUCCESS */
        session.write(inputFlowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream outputStream) throws IOException {
                outputStream.write(jsonContent.getBytes());
            }
        });

        /* session transfer for REL_SUCCESS */
        session.putAttribute(inputFlowFile, "filename", inputFlowFile.getId() + ".json");
        session.transfer(inputFlowFile, REL_SUCCESS);
    }

}

