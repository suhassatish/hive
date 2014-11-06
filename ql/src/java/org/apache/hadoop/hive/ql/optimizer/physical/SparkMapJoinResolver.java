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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.HashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.spark.GenSparkUtils;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.HashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SparkWork;

import com.google.common.base.Preconditions;

/**
 * This class is similar to MapJoinResolver. The difference though, is that
 * we split a SparkWork into two SparkWorks, one containing all the BasWorks for the
 * small tables, and the other containing the BaseWork for the big table.
 *
 * We also set up dependency for the two new SparkWorks.
 */
public class SparkMapJoinResolver implements PhysicalPlanResolver {
  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {

    Dispatcher dispatcher = new SparkMapJoinTaskDispatcher(pctx);
    TaskGraphWalker graphWalker = new TaskGraphWalker(dispatcher);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    graphWalker.startWalking(topNodes, null);
    return pctx;
  }

  class SparkMapJoinTaskDispatcher implements Dispatcher {

    private final PhysicalContext physicalContext;
    private final Map<BaseWork, SparkWork> workMap;
    private final Map<SparkWork, Set<SparkWork>> dependencyMap;

    public SparkMapJoinTaskDispatcher(PhysicalContext pc) {
      physicalContext = pc;
      workMap = new HashMap<BaseWork, SparkWork>();
      dependencyMap = new HashMap<SparkWork, Set<SparkWork>>();
    }

    private boolean containsOp(BaseWork work, Class<?> clazz) {
      for (Operator<? extends OperatorDesc> op : work.getAllOperators()) {
        if (clazz.isInstance(op))
          return true;
        }
      return false;
    }

    private Operator<? extends OperatorDesc> getOp(BaseWork work, Class<?> clazz) {
      for (Operator<? extends OperatorDesc> op : work.getAllOperators()) {
        if (clazz.isInstance(op))
          return op;
        }
      return null;
    }

    // Merge "sourceWork" into "targetWork", also adjust workMap and
    // dependencyMap to reflect this change.
    private void mergeSparkWork(SparkWork sourceWork, SparkWork targetWork) {
      if (sourceWork == targetWork) {
        // DON'T merge self
        return;
      }
      for (BaseWork work : sourceWork.getAllWork()) {
        workMap.put(work, targetWork);
        targetWork.add(work);
        for (BaseWork parentWork : sourceWork.getParents(work)) {
          targetWork.connect(parentWork, work, sourceWork.getEdgeProperty(parentWork, work));
        }
      }

      for (Set<SparkWork> workSet : dependencyMap.values()) {
        if (workSet.contains(sourceWork)) {
          workSet.remove(sourceWork);
          workSet.add(targetWork);
        }
      }

      if (dependencyMap.containsKey(sourceWork)) {
        Set<SparkWork> setToAdd = dependencyMap.get(sourceWork);
        if (!dependencyMap.containsKey(targetWork)) {
          dependencyMap.put(targetWork, new HashSet<SparkWork>());
        }
        dependencyMap.get(targetWork).addAll(setToAdd);
        setToAdd.clear();
      }
    }

    // Create a SparkTask from the input SparkWork, and set up dependency
    // with the information from dependencyMap. If SparkTasks this task depends on
    // are not available yet, recursively compute those.
    private SparkTask createSparkTask(Task<? extends Serializable> currentTask,
                                      SparkWork work, Map<SparkWork, SparkTask> taskMap) {
      if (taskMap.containsKey(work)) {
        return taskMap.get(work);
      }
      SparkTask newTask = (SparkTask) TaskFactory.get(work, physicalContext.conf);
      List<Task<? extends Serializable>> parentTasks = currentTask.getParentTasks();
      if (!dependencyMap.containsKey(work) || dependencyMap.get(work).isEmpty()) {
        if (parentTasks == null) {
          for (Task<? extends Serializable> parentTask : parentTasks) {
            parentTask.addDependentTask(newTask);
            parentTask.removeDependentTask(currentTask);
          }
        } else {
          physicalContext.addToRootTask(newTask);
          physicalContext.removeFromRootTask(currentTask);
        }
      } else {
        for (SparkWork parentWork : dependencyMap.get(work)) {
          SparkTask parentTask = createSparkTask(currentTask, parentWork, taskMap);
          parentTask.addDependentTask(newTask);
        }
      }
      return newTask;
    }

    // Add a dependency edge so that "sourceWork" is dependent on "targetWork"
    private void addDependency(SparkWork sourceWork, SparkWork targetWork) {
      if (!dependencyMap.containsKey(sourceWork)) {
        dependencyMap.put(sourceWork, new HashSet<SparkWork>());
      }
      dependencyMap.get(sourceWork).add(targetWork);
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nos)
        throws SemanticException {
      Task<? extends Serializable> currentTask = (Task<? extends Serializable>) nd;
      if (currentTask instanceof SparkTask
          /*
          TODO- uncomment this condition later - Task.CONVERTED_MAPJOIN
          should get set in CommonJoinResolver
          && currentTask.getTaskTag() == Task.CONVERTED_MAPJOIN */) {

        // Right now, we assume that a work will NOT contain multiple HTS/MJ.
        HiveConf conf = physicalContext.getConf();
        workMap.clear();
        dependencyMap.clear();
        SparkWork sparkWork = ((SparkTask) currentTask).getWork();

        for (BaseWork work : sparkWork.getAllWork()) {
          SparkWork currentSparkWork = new SparkWork(conf.getVar(HiveConf.ConfVars.HIVEQUERYID));
          SparkWork mergedParentSparkWork = null;
          currentSparkWork.add(work);

          for (BaseWork parentWork : sparkWork.getParents(work)) {
            SparkWork parentSparkWork = workMap.get(parentWork);
            if (containsOp(work, MapJoinOperator.class)) {
              MapJoinOperator mjOp = (MapJoinOperator) getOp(work, MapJoinOperator.class);
	            BaseWork modifiedParentWork = replaceReduceSinkWithHashTableSink(parentWork, physicalContext, mjOp);
              if (containsOp(modifiedParentWork, HashTableSinkOperator.class)) {
                if (mergedParentSparkWork == null) {
                  mergedParentSparkWork = parentSparkWork;
                  addDependency(currentSparkWork, mergedParentSparkWork);
                }
              } else {
                mergeSparkWork(parentSparkWork, currentSparkWork);
              }
            } else {
              // current work doesn't contain MJ - we can merge it with the parent work
              mergeSparkWork(parentSparkWork, currentSparkWork);
              currentSparkWork.connect(parentWork, work, sparkWork.getEdgeProperty(parentWork, work));
            }
          }

          workMap.put(work, currentSparkWork);
        }

        // Now create SparkTasks
        // TODO: need to handle ConditionalTask
        Map<SparkWork, SparkTask> taskMap = new HashMap<SparkWork, SparkTask>();
        for (SparkWork work : workMap.values()) {
          createSparkTask(currentTask, work, taskMap);
        }
      }

      return null;
    }

    /**
     * for map-join, replace the reduce sink op with hashTableSink Operator in the operator tree
     * This is partly based on MapJoinResolver.adjustLocalTask in M/R
     * @param smallTableMapWork
     * @param phyCtx
     * @return
     * @throws SemanticException
     */
    private BaseWork replaceReduceSinkWithHashTableSink(BaseWork smallTableMapWork, PhysicalContext phyCtx, MapJoinOperator mjOp)
            throws SemanticException {

    	ParseContext pc = phyCtx.getParseContext();
    	ReduceSinkOperator rsOp = (ReduceSinkOperator) getOp(smallTableMapWork, ReduceSinkOperator.class);
    	MapJoinDesc mjDesc = mjOp.getConf();

    	//TODO: set hashtable memory usage lower if map-join followed by group-by. Investigate if this is relevant for spark
      /*
      HiveConf conf = pc.getConf();
      float hashtableMemoryUsage = conf.getFloatVar(
      HiveConf.ConfVars.HIVEHASHTABLEMAXMEMORYUSAGE);*/

    	HashTableSinkDesc hashTableSinkDesc = new HashTableSinkDesc(mjDesc);
      HashTableSinkOperator hashTableSinkOp = (HashTableSinkOperator) OperatorFactory
          .get(hashTableSinkDesc);

      //get all parents of reduce sink
      List<Operator<? extends OperatorDesc>> parentsOp = rsOp.getParentOperators();
      for (Operator<? extends OperatorDesc> parent : parentsOp) {
      	parent.replaceChild(rsOp, hashTableSinkOp);
      }

      return smallTableMapWork;
    }

  }
}

