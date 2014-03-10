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

package org.apache.flume.sink.hive;


import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.hive.streaming.HiveEndPoint;
import org.apache.hive.streaming.RecordWriter;
import org.apache.hive.streaming.StreamingException;
import org.apache.hive.streaming.TransactionBatch;

import java.io.IOException;

public interface HiveEventSerializer extends Configurable {
  public void write(TransactionBatch batch, Event e) throws StreamingException, IOException;

  RecordWriter createRecordWriter(HiveEndPoint endPoint)
          throws StreamingException, IOException, ClassNotFoundException;

}
