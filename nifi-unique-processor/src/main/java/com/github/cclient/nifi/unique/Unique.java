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
package com.github.cclient.nifi.unique;

import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

/**
 * @author cclient@hotmail.com
 * flowfile distinct by attributes
 */
@Tags({"distinct", "unique", "filter"})
@WritesAttributes({})
@CapabilityDescription("read flowfiles from upStream and distinct by UNIQUE KEY")
@RequiresInstanceClassLoading
public class Unique extends AbstractProcessor {
    public static final PropertyDescriptor UNIQUE_KEY = new PropertyDescriptor.Builder()
            .name("UNIQUE KEY")
            .description("generate unique key")
            .required(true)
            .defaultValue("${uuid}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor RETAIN_FIRST = new PropertyDescriptor.Builder()
            .name("RETAIN FIRST")
            .description("true: same key retain the first one;false:same key retain the last one")
            .required(true)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    public static final PropertyDescriptor BULK_SIZE = new PropertyDescriptor.Builder()
            .name("BULK SIZE")
            .description("0:ready all flowfile from upStream")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully sent will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that fail to send will be routed to this relationship")
            .build();
    protected ComponentLog logger;
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(UNIQUE_KEY);
        properties.add(RETAIN_FIRST);
        properties.add(BULK_SIZE);
        this.properties = Collections.unmodifiableList(properties);
        logger = getLogger();
    }

    @Override
    public Set<Relationship> getRelationships() {
        relationships = new HashSet();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession session) throws ProcessException {
        int bulkSize = processContext.getProperty(BULK_SIZE).asInteger();
        if (bulkSize == 0) {
            bulkSize = Integer.MAX_VALUE;
        }
        List<FlowFile> orginalList = session.get(bulkSize);
        if (orginalList == null || orginalList.size() == 0) {
            return;
        }
        boolean retainFirst = processContext.getProperty(RETAIN_FIRST).asBoolean();
        Map<String, FlowFile> map = new HashMap(orginalList.size());
        List<FlowFile> needRemoveFlowFiles = new ArrayList<>(orginalList.size());
        List<FlowFile> errorFlowFiles = new ArrayList<>(orginalList.size());
        List<FlowFile> needNextFlowFiles = new ArrayList<>(orginalList.size());
        orginalList.forEach(flowFile -> {
            String key = processContext.getProperty(UNIQUE_KEY).evaluateAttributeExpressions(flowFile).getValue();
            if (key == null || key.isEmpty()) {
                errorFlowFiles.add(flowFile);
                return;
            }
            if (map.containsKey(key)) {
                if (retainFirst) {
                    needRemoveFlowFiles.add(flowFile);
                } else {
                    FlowFile oldSame = map.get(key);
                    needRemoveFlowFiles.add(oldSame);
                    needNextFlowFiles.remove(oldSame);
                    needNextFlowFiles.add(flowFile);
                }
            } else {
                needNextFlowFiles.add(flowFile);
                map.put(key, flowFile);
            }
        });
        logger.info("distinct orginal size: {},retain size: {},remove size: {},error size: {}", Arrays.asList(orginalList.size(), needNextFlowFiles.size(), needRemoveFlowFiles.size(), errorFlowFiles.size()).toArray());
        session.transfer(needNextFlowFiles, REL_SUCCESS);
        session.transfer(errorFlowFiles, REL_FAILURE);
        session.remove(needRemoveFlowFiles);
    }
}
