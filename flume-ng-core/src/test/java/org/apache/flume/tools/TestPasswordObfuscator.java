/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.tools;

import junit.framework.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


import java.io.File;
import java.io.IOException;

public class TestPasswordObfuscator {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void test_encodeDecode() throws IOException {
    String originalText = new String("Quick brown fox jumped over the lazy dog. " +
            "the big fat elephant danced around the plump smiling hippo. " +
            "the funky hippie ran up the windy slope.");

    String f = tempFolder.newFile().getAbsolutePath();

    PasswordObfuscator.encodeToFile(originalText, f);
    String decodedText = PasswordObfuscator.readPasswordFromFile(f, PasswordObfuscator.TYPE_AES);
    Assert.assertEquals("Decoded text is not same as original text", originalText, decodedText);
  }
}
