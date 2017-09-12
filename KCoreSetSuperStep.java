package in.dream_lab.goffish.sample;

import in.dream_lab.goffish.api.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Returns core number for each vertex
 *
 * @author Tilak S Naik
 * @author Yogesh Simmhan
 * @version 1.0
 * @see <a href="http://www.dream-lab.in/">DREAM:Lab</a>
 * <p>
 * Copyright 2014 DREAM:Lab, Indian Institute of Science, Bangalore
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class KCoreSetSuperStep extends AbstractSubgraphComputation<LongWritable, LongWritable, LongWritable, KCoreSetSuperStepMessage, LongWritable, LongWritable, LongWritable> implements ISubgraphWrapup {
    private Map<Long, Integer> degrees = new HashMap<>();
    private Map<Long, Integer> cores = new HashMap<>();
    private Map<Long, Set<Long>> subGraphIds = new HashMap<>();
    private Map<Long, Set<Long>> remoteNeighbors = new HashMap<>();
    private Set<Long> verticesToProcess = new HashSet<>();

    @Override
    public void compute(Iterable<IMessage<LongWritable, KCoreSetSuperStepMessage>> iMessages) throws IOException {
        if (getSuperstep() == 0) {
            for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> remoteVertex : getSubgraph().getRemoteVertices())
                remoteNeighbors.put(remoteVertex.getVertexId().get(), new HashSet<Long>());
            for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getLocalVertices()) {
                Collection<IEdge<LongWritable, LongWritable, LongWritable>> edges = (Collection<IEdge<LongWritable, LongWritable, LongWritable>>) vertex.getOutEdges();
                long vertexId = vertex.getVertexId().get();
                int degree = edges.size();
                degrees.put(vertexId, degree);
                cores.put(vertexId, degree + 1);
                subGraphIds.put(vertexId, new HashSet<Long>());
                for (IEdge<LongWritable, LongWritable, LongWritable> edge : edges) {
                    IVertex<LongWritable, LongWritable, LongWritable, LongWritable> neighbor = getSubgraph().getVertexById(edge.getSinkVertexId());
                    if (neighbor.isRemote()) {
                        subGraphIds.get(vertexId).add(((IRemoteVertex<LongWritable, LongWritable, LongWritable, LongWritable, LongWritable>) neighbor).getSubgraphId().get());
                        cores.put(edge.getSinkVertexId().get(), Integer.MAX_VALUE);
                        remoteNeighbors.get(edge.getSinkVertexId().get()).add(vertexId);
                    }
                }
                verticesToProcess.add(vertexId);
            }
        } else {
            for (IMessage<LongWritable, KCoreSetSuperStepMessage> iMessage : iMessages)
                handleMessage(iMessage.getMessage());
        }
        if (calculate())
            voteToHalt();
    }

    private boolean calculate() {
        if (!verticesToProcess.isEmpty()) {
            Set<Long> pendingVertices = new HashSet<>();
            for (long vertexId: verticesToProcess) {
                pendingVertices.remove(vertexId);
                IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex = getSubgraph().getVertexById(new LongWritable(vertexId));
                if (vertex.isRemote())
                    continue;
                if (processVertex(vertex)) {
                    for (IEdge<LongWritable, LongWritable, LongWritable> edge: vertex.getOutEdges())
                        pendingVertices.add(edge.getSinkVertexId().get());
                }
            }
            verticesToProcess = pendingVertices;
        }
        return verticesToProcess.isEmpty();
    }

    private boolean processVertex(IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex) {
        long vertexId = vertex.getVertexId().get();
        int core = cores.get(vertexId);
        int[] count = new int[core + 1];
        for (IEdge<LongWritable, LongWritable, LongWritable> edge : vertex.getOutEdges())
            count[Math.min(core, cores.get(edge.getSinkVertexId().get()))]++;
        int degree = degrees.get(vertexId);
        for (int i = -1; ++i < degree; )
            degree = Math.max(i, degree - count[i]);
        if (degree == core)
            return false;
        cores.put(vertexId, degree);
        informNeighbors(vertexId, degree);
        return true;
    }

    private void informNeighbors(long vertexId, int core) {
        for (long sgId : subGraphIds.get(vertexId))
            sendMessage(new LongWritable(sgId), new KCoreSetSuperStepMessage(vertexId, core));
    }

    private void handleMessage(KCoreSetSuperStepMessage message) {
        long vertexId = message.getVertexId();
        int core = message.getCore();
        if (cores.get(vertexId) <= core)
            return;
        cores.put(vertexId, core);
        verticesToProcess.addAll(remoteNeighbors.get(vertexId));
    }

    @Override
    public void wrapup() throws IOException {
        for (IVertex<LongWritable, LongWritable, LongWritable, LongWritable> vertex : getSubgraph().getLocalVertices())
            System.out.println(vertex.getVertexId().get() + " " + cores.get(vertex.getVertexId().get()));
    }
}

class KCoreSetSuperStepMessage implements Writable {
    private long vertexId;
    private int core;

    public KCoreSetSuperStepMessage() {
    }

    public KCoreSetSuperStepMessage(long vertexId, int core) {
        this.vertexId = vertexId;
        this.core = core;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(vertexId);
        dataOutput.writeInt(core);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        vertexId = dataInput.readLong();
        core = dataInput.readInt();
    }

    public long getVertexId() {
        return vertexId;
    }

    public int getCore() {
        return core;
    }
}
