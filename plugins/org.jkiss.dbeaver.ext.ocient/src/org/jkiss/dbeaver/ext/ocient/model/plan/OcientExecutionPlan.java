/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2019 Serge Rider (serge@jkiss.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jkiss.dbeaver.ext.ocient.model.plan;


import org.jkiss.dbeaver.ext.ocient.model.plan.OcientPlanNodeJson;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.exec.plan.DBCPlanCostNode;
import org.jkiss.dbeaver.model.exec.plan.DBCPlanNode;
import org.jkiss.dbeaver.model.impl.plan.AbstractExecutionPlan;
import org.jkiss.utils.CommonUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Postgre execution plan analyser
 */
public class OcientExecutionPlan extends AbstractExecutionPlan {


    private String query;
    private List<OcientPlanNodeJson> rootNodes;
    
    private static final Gson gson = new Gson();

    public OcientExecutionPlan(String query)
    {
        this.query = query;
    }



    @Override
    public Object getPlanFeature(String feature) {
        if (DBCPlanCostNode.FEATURE_PLAN_COST.equals(feature) ||
            DBCPlanCostNode.FEATURE_PLAN_DURATION.equals(feature) ||
            DBCPlanCostNode.FEATURE_PLAN_ROWS.equals(feature))
        {
            return true;
        }
        return super.getPlanFeature(feature);
    }

    @Override
    public String getQueryString()
    {
        return query;
    }

    @Override
    public String getPlanQueryString() {
    	return query;
    }

    @Override
    public List<? extends DBCPlanNode> getPlanNodes(Map<String, Object> options)
    {
        return rootNodes;
    }

    public void explain(DBCSession session) throws DBCException
    {
    	String explainString = getExplainString(session, getPlanQueryString());
    	
        JsonObject planObject = gson.fromJson(explainString, JsonObject.class);
        JsonObject planRoot = planObject.getAsJsonObject("rootNode");
        rootNodes = new ArrayList<>();  
        
        OcientPlanNodeJson rootNode = new OcientPlanNodeJson(null, "Root", planRoot);        
        rootNodes.add(rootNode);     
    }
    
    private String getExplainString(DBCSession session, String sql) 
    {
    	/*
        JDBCSession connection = (JDBCSession) session;    
        String planJsonFormat;
        try {   			
			Statement stmt = connection.createStatement();
			planJsonFormat = ((XGStatement) stmt).explainJson(getPlanQueryString());
					
        } catch (SQLException e) {
            throw new DBCException(e, session.getDataSource());
        }
		return planJsonFormat;
		*/
    	
    	//explain json select count(*) from montrose.mstats;
    	String plan = "{ \"header\": { \"cols2pos\": { \"count(*)\": 0 }, \"cols2Types\": { \"count(*)\": \"TYPE_BIGINT\" }, \"totalCost\": 200.00009654347065, \"rsSizeInBytes\": \"8\", \"uuid\": \"2b5988d6-748c-4dbd-83a6-4068252f0fac\", \"priority\": 1.0, \"querySeq\": \"1578330112058390451\", \"indexMemUsage\": \"3145734\", \"queryMemUsage\": \"498\", \"version\": 2, \"database\": \"montrose\", \"internalCost\": 100.00009654347065, \"externalCost\": 100.00009654347065 }, \"type\": \"TKT_PLAN\", \"rootNode\": { \"type\": \"ROOT_OPERATOR\", \"id\": \"cc443943-7c3a-4ded-84bf-90d3f477aaca\", \"children\": [{ \"type\": \"REORDER_OPERATOR\", \"id\": \"8883bfad-916d-478d-83c2-feb02082407c\", \"children\": [{ \"type\": \"AGGREGATION_OPERATOR\", \"id\": \"e6025567-bf67-4d5b-b2c3-a433c574a42e\", \"children\": [{ \"type\": \"AGGREGATION_OPERATOR\", \"id\": \"50e0cea1-39db-4bd6-857a-727d845a5593\", \"children\": [{ \"type\": \"GATHER_OPERATOR\", \"id\": \"79b6422e-78f7-4e5d-b472-8d23a9729aa4\", \"children\": [{ \"type\": \"AGGREGATION_OPERATOR\", \"id\": \"8e6b4353-6f77-4303-80c8-e10cca388d1d\", \"children\": [{ \"type\": \"AGGREGATION_OPERATOR\", \"id\": \"891402db-ceb2-4962-a8af-d6396fa3acaf\", \"children\": [{ \"type\": \"GATHER_OPERATOR\", \"id\": \"e5f95255-a4fa-4113-8d32-fc66ce508bf7\", \"children\": [{ \"type\": \"AGGREGATION_OPERATOR\", \"id\": \"d277a51d-f755-4aa0-a2fa-09d75bea33a1\", \"children\": [{ \"type\": \"INDEX_COUNT_STAR_OPERATOR\", \"id\": \"18647fce-caa8-4a0a-97ae-9a72bbd4e2cf\", \"outputColumns\": [\"count(*)\"], \"outputTypes\": [\"TYPE_BIGINT\"], \"colCard\": [\"1\"], \"outputCardinality\": \"1\", \"memoryUsage\": \"3145734\", \"cost\": 100.0, \"sortOrder\": [0], \"sortDirections\": [true], \"outputIsNullable\": [false], \"numParents\": 1, \"colSizes\": [8], \"numDistinctParents\": 1, \"indexCountStarOperator\": { \"schema\": \"sysgdc\", \"tableName\": \"montrose_mstats\", \"indexName\": \"primary_index\", \"db\": \"montrose\", \"tableUuid\": \"d237d41b-cc93-4bdd-b304-e83ac8411f75\", \"storageSpaceUuid\": \"d6be5147-e67c-4475-b214-893b1ff3671a\", \"indexUuid\": \"410bdb74-a124-41cb-a470-9b4967da1382\" } }], \"outputColumns\": [\"count(*)\"], \"outputTypes\": [\"TYPE_BIGINT\"], \"colCard\": [\"1\"], \"outputCardinality\": \"1\", \"cost\": 1.7538093993050353E-5, \"outputIsNullable\": [true], \"numParents\": 1, \"colSizes\": [8], \"numDistinctParents\": 1, \"distanceFromLeaf\": \"1\", \"aggregationOperator\": { \"ops\": [\"AGG_OP_SUM\"], \"inputCols\": [\"count(*)\"], \"outputCols\": [\"count(*)\"], \"outputColsTypes\": [\"LONG\"], \"forceOnePartition\": true } }], \"outputColumns\": [\"count(*)\"], \"outputTypes\": [\"TYPE_BIGINT\"], \"colCard\": [\"1\"], \"outputCardinality\": \"1\", \"memoryUsage\": \"166\", \"cost\": 5.673495679428794E-6, \"outputIsNullable\": [true], \"numParents\": 1, \"colSizes\": [8], \"numDistinctParents\": 1, \"distanceFromLeaf\": \"2\", \"gatherOperator\": { \"level\": \"COORD\" } }], \"outputColumns\": [\"count(*)\"], \"outputTypes\": [\"TYPE_BIGINT\"], \"colCard\": [\"1\"], \"outputCardinality\": \"1\", \"cost\": 4.3845234982625884E-7, \"outputIsNullable\": [true], \"numParents\": 1, \"colSizes\": [8], \"numDistinctParents\": 1, \"distanceFromLeaf\": \"3\", \"aggregationOperator\": { \"ops\": [\"AGG_OP_SUM\"], \"inputCols\": [\"count(*)\"], \"outputCols\": [\"count(*)\"], \"outputColsTypes\": [\"LONG\"] } }], \"outputColumns\": [\"count(*)\"], \"outputTypes\": [\"TYPE_BIGINT\"], \"colCard\": [\"1\"], \"outputCardinality\": \"1\", \"cost\": 1.7538093993050353E-5, \"outputIsNullable\": [true], \"numParents\": 1, \"colSizes\": [8], \"numDistinctParents\": 1, \"distanceFromLeaf\": \"4\", \"aggregationOperator\": { \"ops\": [\"AGG_OP_SUM\"], \"inputCols\": [\"count(*)\"], \"outputCols\": [\"count(*)\"], \"outputColsTypes\": [\"LONG\"], \"forceOnePartition\": true } }], \"outputColumns\": [\"count(*)\"], \"outputTypes\": [\"TYPE_BIGINT\"], \"colCard\": [\"1\"], \"outputCardinality\": \"1\", \"memoryUsage\": \"166\", \"cost\": 5.673495679428794E-6, \"outputIsNullable\": [true], \"numParents\": 1, \"colSizes\": [8], \"numDistinctParents\": 1, \"distanceFromLeaf\": \"5\", \"gatherOperator\": { \"level\": \"SUPER_COORD\" } }], \"outputColumns\": [\"count(*)\"], \"outputTypes\": [\"TYPE_BIGINT\"], \"colCard\": [\"1\"], \"outputCardinality\": \"1\", \"cost\": 4.3845234982625884E-7, \"outputIsNullable\": [true], \"numParents\": 1, \"colSizes\": [8], \"numDistinctParents\": 1, \"distanceFromLeaf\": \"6\", \"aggregationOperator\": { \"ops\": [\"AGG_OP_SUM\"], \"inputCols\": [\"count(*)\"], \"outputCols\": [\"count(*)\"], \"outputColsTypes\": [\"LONG\"] } }], \"outputColumns\": [\"count(*)\"], \"outputTypes\": [\"TYPE_BIGINT\"], \"colCard\": [\"1\"], \"outputCardinality\": \"1\", \"cost\": 1.7538093993050353E-5, \"outputIsNullable\": [true], \"numParents\": 1, \"colSizes\": [8], \"numDistinctParents\": 1, \"distanceFromLeaf\": \"7\", \"aggregationOperator\": { \"ops\": [\"AGG_OP_SUM\"], \"inputCols\": [\"count(*)\"], \"outputCols\": [\"count(*)\"], \"outputColsTypes\": [\"LONG\"], \"forceOnePartition\": true } }], \"outputColumns\": [\"count(*)\"], \"outputTypes\": [\"TYPE_BIGINT\"], \"colCard\": [\"1\"], \"outputCardinality\": \"1\", \"cost\": 100.0, \"outputIsNullable\": [true], \"numParents\": 1, \"colSizes\": [8], \"numDistinctParents\": 1, \"distanceFromLeaf\": \"8\", \"reorderOperator\": { \"columns\": [\"count(*)\"] } }], \"outputColumns\": [\"count(*)\"], \"outputTypes\": [\"TYPE_BIGINT\"], \"colCard\": [\"1\"], \"outputCardinality\": \"1\", \"memoryUsage\": \"166\", \"cost\": 3.170529259401351E-5, \"outputIsNullable\": [true], \"colSizes\": [8], \"numDistinctParents\": 1, \"distanceFromLeaf\": \"9\", \"rootOperator\": { } } }";
    	return plan;
    }
    
    

}
