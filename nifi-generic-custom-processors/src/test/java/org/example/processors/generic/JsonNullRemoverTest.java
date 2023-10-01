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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;


/**
 * @author AJ
 * @implNote check to verify processors output
 */
public class JsonNullRemoverTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(JsonNullRemover.class );
    }

    @Test
    public void outputTesterJsonNullRemover() {
        /* Read the content of a sample JSON file */
        String jsonString = "{\"name\":\"John\",\"age\":30,\"address\":null,\"phone\":\"  \", \"email\":\" \"}";

        // Add the content to the runner
        testRunner.enqueue(jsonString);

        /* Run the enqueued content, it also takes an int = number of contents queued */
        testRunner.run(1);

        /* All results were processed without failure */
        testRunner.assertQueueEmpty();

        /* If you need to read or do additional tests on results you can access the content */
        List<MockFlowFile> mockFlowFileList = testRunner.getFlowFilesForRelationship(JsonNullRemover.REL_SUCCESS);
        assertTrue("1 match", mockFlowFileList.size() == 1);
        MockFlowFile MockFileREL_SUCCESS = mockFlowFileList.get(0);
        String resultJson = IOUtils.toString(testRunner.getContentAsByteArray(MockFileREL_SUCCESS));

        System.out.println("results: " + resultJson);

       /* to test it locally
        JsonObject json = new Gson().fromJson(resultJson,JsonObject.class);
        File fjj = new File("test_Json.json");
        FileWriter fjonj = new FileWriter(fjj);
        fjonj.write(String.valueOf(json));
        fjonj.close();*/
    }

}
