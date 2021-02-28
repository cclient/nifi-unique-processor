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

import com.github.cclient.nifi.unique.Unique;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUnique {

    private TestRunner runner;

    @Before
    public void init() {
        runner = TestRunners.newTestRunner(Unique.class);

        Map<String, String> attrs0 = new HashMap<>();
        attrs0.put("custom_id", "1");
        attrs0.put("custom_value", "123");

        Map<String, String> attrs1 = new HashMap<>();
        attrs1.put("custom_id", "1");
        attrs1.put("custom_value", "456");

        Map<String, String> attrs2 = new HashMap<>();
        attrs2.put("custom_id", "2");
        attrs2.put("custom_value", "789");

        MockFlowFile flowFile0 = new MockFlowFile(0);
        MockFlowFile flowFile1 = new MockFlowFile(1);
        MockFlowFile flowFile2 = new MockFlowFile(2);

        flowFile0.putAttributes(attrs0);
        flowFile1.putAttributes(attrs1);
        flowFile2.putAttributes(attrs2);
        runner.enqueue(flowFile0, flowFile1, flowFile2);


    }

    @Test
    public void testRetainFirstProcessor() {
        runner.setProperty(Unique.BULK_SIZE, "0");
        runner.setProperty(Unique.RETAIN_FIRST, "true");
        runner.setProperty(Unique.UNIQUE_KEY, "${custom_id}");
        runner.run();
        List<MockFlowFile> result = runner.getFlowFilesForRelationship(Unique.REL_SUCCESS);
        Assert.assertEquals(2, result.size());
        MockFlowFile flowFile0=result.get(0);
        MockFlowFile flowFile1=result.get(1);
        System.out.println("flowfile custom_id: "+flowFile0.getAttribute("custom_id")+" custom_value: "+flowFile0.getAttribute("custom_value"));
        System.out.println("flowfile custom_id: "+flowFile1.getAttribute("custom_id")+" custom_value: "+flowFile1.getAttribute("custom_value"));
        Assert.assertEquals("123", flowFile0.getAttribute("custom_value"));

    }

    @Test
    public void testRetainLastProcessor() {
        runner.setProperty(Unique.BULK_SIZE, "0");
        runner.setProperty(Unique.RETAIN_FIRST, "false");
        runner.setProperty(Unique.UNIQUE_KEY, "${custom_id}");
        runner.run();
        List<MockFlowFile> result = runner.getFlowFilesForRelationship(Unique.REL_SUCCESS);
        Assert.assertEquals(2, result.size());
        MockFlowFile flowFile0=result.get(0);
        MockFlowFile flowFile1=result.get(1);
        System.out.println("flowfile custom_id: "+flowFile0.getAttribute("custom_id")+" custom_value: "+flowFile0.getAttribute("custom_value"));
        System.out.println("flowfile custom_id: "+flowFile1.getAttribute("custom_id")+" custom_value: "+flowFile1.getAttribute("custom_value"));
        Assert.assertEquals("456", flowFile0.getAttribute("custom_value"));
    }
}
