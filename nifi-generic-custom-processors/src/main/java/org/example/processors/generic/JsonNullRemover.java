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
import com.google.gson.JsonParser;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.example.processors.generic.util.Constant;
import org.example.processors.generic.util.Helper;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"json,null,remover"})
@CapabilityDescription("Remove null fields from Json Objects")
@EventDriven
@SupportsBatching
@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class JsonNullRemover extends AbstractProcessor {


    /* custom relationship declaration and assignment*/
    static final Relationship REL_SUCCESS = new Relationship.Builder().name(Constant.REL_SUCCESS_NAME)
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name(Constant.REL_FAILED_NAME).build();


    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;


    /* processor initialization method */
    @Override
    protected void init(final ProcessorInitializationContext context) {
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
            String jsonString = Helper.flowFileReader(inputFlowFile, session);
            getLogger().info("jsonContent {}", jsonString);

            // Parse the JSON string to a JsonObject
           JsonObject jsonObject = JsonParser.parseString(jsonString).getAsJsonObject();
           // Remove null, empty, and whitespace fields
           if (jsonObject != null) {
               Helper.removeNullEmptyAndWhitespaceFields(jsonObject);
           }
           // Convert the JsonObject back to a JSON string
           String updatedJsonString = new Gson().toJson(jsonObject);
           // Print the updated JSON string
           getLogger().info("updated Json {}", jsonString);
           getLogger().info("sessionTransfer ...");
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

