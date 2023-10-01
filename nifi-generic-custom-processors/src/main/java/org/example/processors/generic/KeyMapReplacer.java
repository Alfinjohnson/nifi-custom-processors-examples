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
import org.example.processors.generic.util.CustomValidator;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

import static org.example.processors.generic.util.Constant.*;
import static org.example.processors.generic.util.Helper.*;

@Tags({"json,key,map"})
@CapabilityDescription("Json Key value replacer")
@EventDriven
@SupportsBatching
@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class KeyMapReplacer extends AbstractProcessor {

    /* property builder function  */

    static final PropertyDescriptor CONST_MAP_KEYS;
    static final PropertyDescriptor CONST_MAP_VALUES;
    static final PropertyDescriptor KEY_TO_CHECK;
    static final PropertyDescriptor NEW_KEY_NAMES;

    private static String propertyValueMapKeys;
    private static String propertyValueMapValues;

    private static String[] newKeyNamesArray;
    private static String[] keyToCheckArray;

    /* custom relationship declaration and assignment*/
    static final Relationship REL_SUCCESS = new Relationship.Builder().name(REL_SUCCESS_NAME)
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name(REL_FAILED_NAME).build();


    static {
        PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder();
        builder.name(KEY_NAMES);
        builder.displayName(KEY_NAMES);
        builder.description(KEY_NAMES_DESCRIPTION);
        builder.required(true);
        builder.defaultValue(KEY_NAMES_PROPERTY_EXAMPLE);
        builder.addValidator(CustomValidator::customPropertyValidator);
        CONST_MAP_KEYS = builder
                .build();
    }

    static {
        PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder();
        builder.name(KEY_VALUES);
        builder.displayName(KEY_VALUES);
        builder.description(KEY_VALUES_DESCRIPTION);
        builder.defaultValue(KEY_VALUES_PROPERTY_EXAMPLE);
        builder.required(true);
        builder.addValidator(CustomValidator::customPropertyValidator);
        CONST_MAP_VALUES = builder
                .build();
    }
    static {
        PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder();
        builder.name(KEY_TO_CHECK_NAME);
        builder.displayName(KEY_TO_CHECK_NAME);
        builder.description(KEY_TO_CHECK_NAME_DESCRIPTION);
        builder.defaultValue(KEY_TO_CHECK_PROPERTY_EXAMPLE);
        builder.required(true);
        builder.addValidator(CustomValidator::customPropertyValidator);
        KEY_TO_CHECK = builder
                .build();
    }

    static {
        PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder();
        builder.name(NEW_KEY_NAMES_DISPLAY);
        builder.displayName(NEW_KEY_NAMES_DISPLAY);
        builder.description(NEW_KEY_NAMES_DESCRIPTION);
        builder.defaultValue(NEW_KEY_NAMES_PROPERTY_EXAMPLE);
        builder.required(true);
        builder.addValidator(CustomValidator::customPropertyValidator);
        NEW_KEY_NAMES = builder
                .build();
    }

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;


    /* processor initialization method */
    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();

        /* intRelationships and propertyDescriptors  ProcessorInitialization */
        propertyDescriptors.add(NEW_KEY_NAMES);
        propertyDescriptors.add(KEY_TO_CHECK);
        propertyDescriptors.add(CONST_MAP_VALUES);
        propertyDescriptors.add(CONST_MAP_KEYS);
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
        propertyValueMapKeys = context.getProperty(CONST_MAP_KEYS).getValue();
        propertyValueMapValues = context.getProperty(CONST_MAP_VALUES).getValue();
        String keyToCheck = context.getProperty(KEY_TO_CHECK).getValue();
        String newKeyNames = context.getProperty(NEW_KEY_NAMES).getValue();
        getLogger().info("input values : propertyValueMapKeys {}, propertyValueMapValues {}, keyToCheck {}, newKeyNames {} ", propertyValueMapKeys,propertyValueMapValues, keyToCheck, newKeyNames);

        newKeyNamesArray = newKeyNames.split(",");
        keyToCheckArray = keyToCheck.split(",");
        getLogger().info("newKeyNamesArray {}", newKeyNamesArray);
        getLogger().info("keyToCheckArray {}", keyToCheckArray);
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




        if (keyToCheckArray.length != newKeyNamesArray.length) throw new IllegalArgumentException("keyToCheck and newKeyNames not matched at length");
        try {
            getLogger().info("reading json file from inputFlowFile");
            String jsonContent = flowFileReader(inputFlowFile, session);
            getLogger().info("jsonContent {}", jsonContent);

            // Creating a map to store the replacement key-value pairs
            getLogger().info("Create a map to store the replacement key-value pairs");
            final Map<String, String> replacementMap = createReplacementMap(propertyValueMapKeys, propertyValueMapValues);
            getLogger().info("new Map {}", replacementMap);

            String newJson = addMapValueToJSON(jsonContent, replacementMap, keyToCheckArray , newKeyNamesArray);
            getLogger().info("newKeyNamesArray {}", newKeyNamesArray);
            getLogger().info("keyToCheckArray {}", keyToCheckArray);

            sessionTransfer(inputFlowFile, session, newJson);
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

