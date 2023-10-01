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
import org.example.processors.generic.util.Helper;
import org.jetbrains.annotations.NotNull;
import org.example.processors.generic.util.Constant;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

@Tags({"json,regex,replacer"})
@CapabilityDescription("Replace specified json key-value using regex pattern")
@EventDriven
@SupportsBatching
@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class RangeConverter extends AbstractProcessor {

    /* property builder function  */

    static final PropertyDescriptor SpecifiedKey;
    static final PropertyDescriptor SpecifiedNewKey;

    private static String specifiedKey;
    private static String  specifiedNewKey;

    /* custom relationship declaration and assignment*/
    static final Relationship REL_SUCCESS = new Relationship.Builder().name(Constant.REL_SUCCESS_NAME)
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name(Constant.REL_FAILED_NAME).build();


    static {
        PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder();
        builder.name(Constant.RANGE_REGEX_NAME);
        builder.displayName(Constant.RANGE_REGEX_NAME);
        builder.description(Constant.RANGE_REGEX_NAMES_DESCRIPTION);
        builder.required(true);
        builder.defaultValue(Constant.RANGE_REGEX_NAMES_PROPERTY_EXAMPLE);
        builder.addValidator(StandardValidators.NON_EMPTY_VALIDATOR);
        SpecifiedKey = builder
                .build();
    }

    static {
        PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder();
        builder.name(Constant.RANGE_FIELD_NAME);
        builder.displayName(Constant.RANGE_FIELD_NAME);
        builder.description(Constant.RANGE_FIELD_DESCRIPTION);
        builder.defaultValue(Constant.RANGE_FIELD_PROPERTY_EXAMPLE);
        builder.required(true);
        builder.addValidator(StandardValidators.NON_EMPTY_VALIDATOR);
        SpecifiedNewKey = builder
                .build();
    }
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;


    /* processor initialization method */
    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();

        /* intRelationships and propertyDescriptors  ProcessorInitialization */
        propertyDescriptors.add(SpecifiedNewKey);
        propertyDescriptors.add(SpecifiedKey);
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
        specifiedKey = context.getProperty(SpecifiedKey).getValue();
        specifiedNewKey = context.getProperty(SpecifiedNewKey).getValue();
        getLogger().info("input values : specifiedKey {}, specifiedNewKey {} ", specifiedKey,specifiedNewKey );

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
            String jsonContent = Helper.flowFileReader(inputFlowFile, session);
            getLogger().info("jsonContent {}", jsonContent);

            Gson gson = new Gson();
            JsonObject jsonObject = gson.fromJson(jsonContent, JsonObject.class);
            double inputValue = jsonObject.get(specifiedKey).getAsDouble();

            String range = Helper.convertToRange(inputValue);
            jsonObject.addProperty(specifiedNewKey, range);

            String updatedJson = gson.toJson(jsonObject);
            getLogger().info("Updated JSON: " + updatedJson);


            getLogger().info("sessionTransfer .");
            sessionTransfer(inputFlowFile, session, updatedJson);
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

