/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.source;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.List;

public class TestNetcatSource {

  private NetcatSource source;
  private Channel channel;

  /**
   * We set up the the Netcat source and Flume Memory Channel on localhost
   *
   * @throws UnknownHostException
   */
  @Before
  public void setUp() throws UnknownHostException {
    source = new NetcatSource();
    channel = new MemoryChannel();

    Configurables.configure(channel, new Context());

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
  }

  /**
   * Tests that the source is stopped when an exception is thrown
   * on port bind attempt due to port already being in use.
   *
   * @throws InterruptedException
   */
  @Test
  public void testSourceStoppedOnFlumeException() throws InterruptedException, IOException {
    boolean isFlumeExceptionThrown = false;
    // create a dummy socket bound to a known port.
    try (ServerSocketChannel dummyServerSocket = ServerSocketChannel.open()) {
      dummyServerSocket.socket().setReuseAddress(true);
      dummyServerSocket.socket().bind(new InetSocketAddress("0.0.0.0", 10500));

      Context context = new Context();
      context.put("port", String.valueOf(10500));
      context.put("bind", "0.0.0.0");
      context.put("ack-every-event", "false");
      Configurables.configure(source, context);

      source.start();
    } catch (FlumeException fe) {
      isFlumeExceptionThrown = true;
    }
    // As port is already in use, an exception is thrown and the source is stopped
    // cleaning up the opened sockets during source.start().
    Assert.assertTrue("Flume exception is thrown as port already in use", isFlumeExceptionThrown);
    Assert.assertEquals("Server is stopped", LifecycleState.STOP,
        source.getLifecycleState());
  }

}
