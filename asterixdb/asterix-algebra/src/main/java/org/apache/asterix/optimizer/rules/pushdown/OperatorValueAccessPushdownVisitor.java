/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.optimizer.rules.pushdown;

import java.util.*;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.om.base.AGeometry;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.base.AsterixOptimizationContext;
import org.apache.asterix.optimizer.rules.pushdown.schema.RootExpectedSchemaNode;
import org.apache.asterix.runtime.projection.DataProjectionInfo;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IndexInsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IntersectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.MaterializeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.RunningAggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ScriptOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SinkOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TokenizeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WindowOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.WriteResultOperator;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

import com.esri.core.geometry.Envelope;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;

/**
 * This visitor visits the entire plan and tries to build the information of the required values from all dataset
 */
public class OperatorValueAccessPushdownVisitor implements ILogicalOperatorVisitor<Void, Void> {

    private final IOptimizationContext context;
    //Requested schema builder. It is only expected schema not a definite one
    private final ExpectedSchemaBuilder builder;
    //To visit every expression in each operator
    private final ExpressionValueAccessPushdownVisitor pushdownVisitor;
    //Datasets that allow pushdowns
    private final Map<LogicalVariable, DataSourceScanOperator> registeredDatasets;
    //visitedOperators so we do not visit the same operator twice (in case of REPLICATE)
    private final Set<ILogicalOperator> visitedOperators;
    //bounding box co-ordiantes of filter
    private boolean hasFilterPushdown;
    private double xMin;
    private double yMin;
    private double xMax;
    private double yMax;

    public OperatorValueAccessPushdownVisitor(IOptimizationContext context) {
        this.context = context;
        builder = new ExpectedSchemaBuilder();
        registeredDatasets = new HashMap<>();
        pushdownVisitor = new ExpressionValueAccessPushdownVisitor(builder);
        visitedOperators = new HashSet<>();
        hasFilterPushdown = false;
    }

    public void finish() throws AlgebricksException {
        for (Map.Entry<LogicalVariable, DataSourceScanOperator> scan : registeredDatasets.entrySet()) {
            scan.getValue().setProjectionInfo(builder.createProjectionInfo(scan.getKey()));
            if (hasFilterPushdown) {
                DataProjectionInfo projectionInfo = (DataProjectionInfo) scan.getValue().getProjectionInfo();
                projectionInfo.setFilterMBR(xMin, yMin, xMax, yMax);
            }
        }
    }

    /**
     * Visit every input of an operator. Then, start pushdown any value expression that the operator has
     *
     * @param op                the operator to process
     * @param producedVariables any produced variables by the operator. We only care about the {@link AssignOperator}
     *                          and {@link UnnestOperator} variables for now.
     */
    private void visitInputs(ILogicalOperator op, List<LogicalVariable> producedVariables) throws AlgebricksException {
        if (visitedOperators.contains(op)) {
            return;
        }
        for (Mutable<ILogicalOperator> child : op.getInputs()) {
            child.getValue().accept(this, null);
        }
        visitedOperators.add(op);
        //Initiate the pushdown visitor
        pushdownVisitor.init(producedVariables);
        //pushdown any expression the operator has
        op.acceptExpressionTransform(pushdownVisitor);
        pushdownVisitor.end();
    }

    /*
     * ******************************************************************************
     * Operators that need to handle special cases
     * ******************************************************************************
     */

    @Override
    public Void visitProjectOperator(ProjectOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        if (op.getVariables().isEmpty()) {
            //If the variables are empty and the next operator is DataSourceScanOperator, then set empty record
            setEmptyRecord(op.getInputs().get(0).getValue());
        }
        return null;
    }

    /**
     * From the {@link DataSourceScanOperator}, we need to register the payload variable (record variable) to check
     * which expression in the plan is using it.
     */
    @Override
    public Void visitDataScanOperator(DataSourceScanOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        DatasetDataSource datasetDataSource = getDatasetDataSourceIfApplicable(op);
        if (datasetDataSource != null) {
            LogicalVariable recordVar = datasetDataSource.getDataRecordVariable(op.getVariables());
            if (!builder.isVariableRegistered(recordVar)) {
                /*
                 * This is the first time we see the dataset, and we know we might only need part of the record.
                 * Register the dataset to prepare for value access expression pushdowns.
                 * Initially, we will request the entire record.
                 */
                builder.registerDataset(recordVar, RootExpectedSchemaNode.ALL_FIELDS_ROOT_NODE);
                registeredDatasets.put(recordVar, op);
            }
        }
        return null;
    }

    @Override
    public Void visitAggregateOperator(AggregateOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        if (!op.isGlobal() && isCountConstant(op.getExpressions())) {
            /*
             * Optimize the SELECT COUNT(*) case
             * It is local aggregate and has agg-sql-count function with a constant argument. Set empty record if the
             * input operator is DataSourceScanOperator
             */
            setEmptyRecord(op.getInputs().get(0).getValue());
        }
        return null;
    }

    /*
     * ******************************************************************************
     * Helper methods
     * ******************************************************************************
     */

    /**
     * The role of this method is:
     * 1- Check whether the dataset is an external dataset and allows value access pushdowns
     * 2- return the actual DatasetDataSource
     */
    private DatasetDataSource getDatasetDataSourceIfApplicable(DataSourceScanOperator scan) throws AlgebricksException {
        DataSource dataSource = (DataSource) scan.getDataSource();
        if (dataSource == null) {
            return null;
        }

        MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();
        DataverseName dataverse = dataSource.getId().getDataverseName();
        String datasetName = dataSource.getId().getDatasourceName();
        Dataset dataset = mp.findDataset(dataverse, datasetName);

        //Only external dataset can have pushed down expressions
        if (dataset == null || dataset.getDatasetType() == DatasetConfig.DatasetType.INTERNAL
                || dataset.getDatasetType() == DatasetConfig.DatasetType.EXTERNAL && !ExternalDataUtils
                        .supportsPushdown(((ExternalDatasetDetails) dataset.getDatasetDetails()).getProperties())) {
            return null;
        }

        return (DatasetDataSource) dataSource;
    }

    /**
     * If the inputOp is a {@link DataSourceScanOperator}, then set the projected value needed as empty record
     *
     * @param inputOp an operator that is potentially a {@link DataSourceScanOperator}
     * @see #visitAggregateOperator(AggregateOperator, Void)
     * @see #visitProjectOperator(ProjectOperator, Void)
     */
    private void setEmptyRecord(ILogicalOperator inputOp) throws AlgebricksException {
        if (inputOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            DataSourceScanOperator scan = (DataSourceScanOperator) inputOp;
            DatasetDataSource datasetDataSource = getDatasetDataSourceIfApplicable(scan);
            if (datasetDataSource != null) {
                //We know that we only need the count of objects. So return empty objects only
                LogicalVariable recordVar = datasetDataSource.getDataRecordVariable(scan.getVariables());
                /*
                 * Set the root node as EMPTY_ROOT_NODE (i.e., no fields will be read from disk). We register the
                 * dataset with EMPTY_ROOT_NODE so that we skip pushdowns on empty node.
                 */
                builder.registerDataset(recordVar, RootExpectedSchemaNode.EMPTY_ROOT_NODE);
            }
        }
    }

    private boolean isCountConstant(List<Mutable<ILogicalExpression>> expressions) {
        if (expressions.size() != 1) {
            return false;
        }
        ILogicalExpression expression = expressions.get(0).getValue();
        if (expression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression;
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();
        return BuiltinFunctions.SQL_COUNT.equals(fid)
                && funcExpr.getArguments().get(0).getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT;
    }

    private void visitSubplans(List<ILogicalPlan> nestedPlans) throws AlgebricksException {
        for (ILogicalPlan plan : nestedPlans) {
            for (Mutable<ILogicalOperator> root : plan.getRoots()) {
                visitInputs(root.getValue());
            }
        }
    }

    /*
     * ******************************************************************************
     * Pushdown when possible for each operator
     * ******************************************************************************
     */

    @Override
    public Void visitAssignOperator(AssignOperator op, Void arg) throws AlgebricksException {
        visitInputs(op, op.getVariables());
        return null;
    }

    @Override
    public Void visitSelectOperator(SelectOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);

        if (isShapeFileFormat()) {
            ArrayList<String> acceptedFunctionNames = new ArrayList<String>(
                    Arrays.asList("st-intersects", "st-contains", "st-crosses", "st-equals", "st-overlaps", "st-touches", "st-within"));
            ILogicalExpression logicalExpression = op.getCondition().getValue();
            if (logicalExpression instanceof ScalarFunctionCallExpression) {
                String functionName = ((ScalarFunctionCallExpression) logicalExpression).getFunctionIdentifier().getName();
                if(acceptedFunctionNames.contains(functionName)){
                    List<Mutable<ILogicalExpression>> arguments =
                            ((ScalarFunctionCallExpression) logicalExpression).getArguments();
                    for (Mutable<ILogicalExpression> e : arguments) {
                        ILogicalExpression argument = e.getValue();
                        if (argument instanceof ConstantExpression) {
                            IAlgebricksConstantValue value = ((ConstantExpression) argument).getValue();
                            if (value instanceof AsterixConstantValue) {
                                IAObject constantObject = ((AsterixConstantValue) value).getObject();
                                if (constantObject instanceof AGeometry) {
                                    hasFilterPushdown = true;
                                    Envelope record = new Envelope();
                                    ((AGeometry) constantObject).getGeometry().getEsriGeometry().queryEnvelope(record);
                                    xMin = record.getXMin();
                                    yMin = record.getYMin();
                                    xMax = record.getXMax();
                                    yMax = record.getYMax();
                                }
                            }
                        }
                    }
                }
            }
        }

        return null;
    }

    @Override
    public Void visitSubplanOperator(SubplanOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        visitSubplans(op.getNestedPlans());
        return null;
    }

    @Override
    public Void visitUnnestOperator(UnnestOperator op, Void arg) throws AlgebricksException {
        visitInputs(op, op.getVariables());
        return null;
    }

    @Override
    public Void visitRunningAggregateOperator(RunningAggregateOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitEmptyTupleSourceOperator(EmptyTupleSourceOperator op, Void arg) throws AlgebricksException {
        return null;
    }

    @Override
    public Void visitGroupByOperator(GroupByOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        visitSubplans(op.getNestedPlans());
        return null;
    }

    @Override
    public Void visitLimitOperator(LimitOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitInnerJoinOperator(InnerJoinOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitLeftOuterJoinOperator(LeftOuterJoinOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitNestedTupleSourceOperator(NestedTupleSourceOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitOrderOperator(OrderOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitDelegateOperator(DelegateOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitReplicateOperator(ReplicateOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitSplitOperator(SplitOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitMaterializeOperator(MaterializeOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitScriptOperator(ScriptOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitSinkOperator(SinkOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitUnionOperator(UnionAllOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitIntersectOperator(IntersectOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestOperator(LeftOuterUnnestOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitUnnestMapOperator(UnnestMapOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitLeftOuterUnnestMapOperator(LeftOuterUnnestMapOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitDistinctOperator(DistinctOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitExchangeOperator(ExchangeOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitWriteOperator(WriteOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitDistributeResultOperator(DistributeResultOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitWriteResultOperator(WriteResultOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitInsertDeleteUpsertOperator(InsertDeleteUpsertOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitIndexInsertDeleteUpsertOperator(IndexInsertDeleteUpsertOperator op, Void arg)
            throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitTokenizeOperator(TokenizeOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitForwardOperator(ForwardOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        return null;
    }

    @Override
    public Void visitWindowOperator(WindowOperator op, Void arg) throws AlgebricksException {
        visitInputs(op);
        visitSubplans(op.getNestedPlans());
        return null;
    }

    private void visitInputs(ILogicalOperator op) throws AlgebricksException {
        visitInputs(op, null);
    }

    private boolean isShapeFileFormat() throws AlgebricksException {
        ObjectSet<Int2ObjectMap.Entry<Set<DataSource>>> entrySet =
                ((AsterixOptimizationContext) context).getDataSourceMap().int2ObjectEntrySet();
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        for (Int2ObjectMap.Entry<Set<DataSource>> dataSources : entrySet) {
            for (DataSource dataSource : dataSources.getValue()) {
                DataverseName dataverse = dataSource.getId().getDataverseName();
                String dataSetName = dataSource.getId().getDatasourceName();
                Dataset dataset = metadataProvider.findDataset(dataverse, dataSetName);
                if (ExternalDataUtils
                        .isShapeFileFormat(((ExternalDatasetDetails) dataset.getDatasetDetails()).getProperties())) {
                    return true;
                }
            }
        }
        return false;
    }
}
