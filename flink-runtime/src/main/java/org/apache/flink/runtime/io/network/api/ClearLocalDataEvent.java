/*
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

package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

/**
 * @author zhangzhengqi5
 */
public class ClearLocalDataEvent extends RuntimeEvent {

    /** The singleton instance of this event. */
    public static final ClearLocalDataEvent INSTANCE = new ClearLocalDataEvent();

    // ------------------------------------------------------------------------

    // not instantiable
    private ClearLocalDataEvent() {}

    // ------------------------------------------------------------------------

    @Override
    public void read(DataInputView in) {
        // Nothing to do here
    }

    @Override
    public void write(DataOutputView out) {
        // Nothing to do here
    }

    // ------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return 5535;
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && obj.getClass() == ClearLocalDataEvent.class;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
