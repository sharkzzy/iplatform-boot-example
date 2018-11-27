/*
* 张磊 2018.11.27
* */
package org.iplatform.microservices.core.neo4j;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.iplatform.microservices.core.neo4j.dto.Node;
import org.iplatform.microservices.core.neo4j.dto.Path;
import org.iplatform.microservices.core.neo4j.dto.Relationship;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Component
@ConditionalOnProperty(name = "spring.data.neo4j.uri")
public class Neo4jGraphService {
    private static Logger LOG = LoggerFactory.getLogger(Neo4jGraphService.class);

    @Autowired
    private Neo4jSessionFactory neo4jSessionFactory;

    private ThreadLocal<Transaction> transactions = new ThreadLocal<>();

    private ThreadLocal<AtomicLong> nestedCounter = new ThreadLocal<>();

    public void begin() {
        if(nestedCounter.get()==null){
            nestedCounter.set(new AtomicLong(1));
        }else{
            nestedCounter.get().incrementAndGet();
        }
        if (transactions.get() == null) {
            this.transactions.set(neo4jSessionFactory.getNeo4jSession().beginTransaction());
        }
    }

    public void commit() {
        long counter = nestedCounter.get().decrementAndGet();
        Transaction transaction = transactions.get();
        if (transaction != null) {
            try {
                if(counter<=0){
                    transaction.success();
                    transaction.close();
                }
            } finally {
                if(counter<=0){
                    nestedCounter.remove();
                }
                transactions.remove();
            }
        }
    }

    public void rollback() {
        long counter = nestedCounter.get().decrementAndGet();
        Transaction transaction = transactions.get();
        if (transaction != null) {
            try {
                if(counter<=0){
                    transaction.failure();
                }
            } finally {
                if(counter<=0){
                    nestedCounter.remove();
                }
                transactions.remove();
            }
        }
    }

    private StatementResult run(String cypher) {
        return run(cypher, Collections.EMPTY_MAP);
    }

    private StatementResult run(String cypher, Map<String, Object> statementParameters) {
        LOG.info("Cypher: {}", cypher);
        StatementRunner runner = null;
        boolean isTransaction = Boolean.TRUE;
        try {
            runner = transactions.get();
            if (runner == null) {
                isTransaction = Boolean.FALSE;
                runner = neo4jSessionFactory.getNeo4jSession();
            }
            return runner.run(cypher, statementParameters);
        } finally {
            if (runner != null) {
                if (!isTransaction) {
                    ((Session) runner).close();
                }
            }
        }
    }

    /*
    * 增加修改一个点
    * */
    public void addNode(Node node) {
        StringBuffer cypher = new StringBuffer();
        cypher.append("CREATE (");
        cypher.append(node.getName() + ":" + node.getLabels());
        cypher.append(node.getProperitesJSON());
        cypher.append(")");
        run(cypher.toString());
    }

    /*
    * 判断点是否存在
    * */
    public Boolean existNode(Node node) {
        return getNode(node) == null ? Boolean.FALSE : Boolean.TRUE;
    }

    public Node getNode(Node node) {
        StringBuffer cypher = new StringBuffer();
        cypher.append("MATCH(n:" + node.getLabels() + " {id:'" + node.getId() + "'} RETURN n");
        StatementResult result = run(cypher.toString());
        return null;
    }

    /*
    * 删除点
    * */
    public void delete(Node node) {
        StringBuffer cypher = new StringBuffer();
        cypher.append("MATCH(n:" + node.getLabels() + " {id:'" + node.getId() + "'} DELETE n");
        run(cypher.toString());
    }

    /*
    * 删除所有数据
    * */
    public void deleteAll() {
        StringBuffer cypher = new StringBuffer();
        cypher.append("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r");
        run(cypher.toString());
    }

    /*
    * 增加一个关系
    * */
    public void addRelationship(Relationship relationship) {
        StringBuffer cypher = new StringBuffer();
        cypher.append("MATCH (n {id:'" + relationship.getStartId() + "'}) ");
        cypher.append("MATCH (m {id:'" + relationship.getEndId() + "'}) ");
        cypher.append("CREATE (n)-[r:" + relationship.getType() + relationship.getProperitesJSON() + "]->(m) RETURN r");
        run(cypher.toString());
    }

    /*
    * 删除两点间所有关系
    * */
    public void deleteRelationship(String startId, String EndId,DirectionEnum direction){
        StringBuffer cypher = new StringBuffer();
        cypher.append("MATCH (n {id:'"+startId+"'})");
        if(direction== DirectionEnum.OUTGOING){
            cypher.append("-[r]->");
        }else if(direction== DirectionEnum.INCOMING){
            cypher.append("<-[r]-");
        }else{
            cypher.append("-[r]-");
        }
        cypher.append("(m {id:'"+EndId+"'}) DELETE r");
        run(cypher.toString());
    }

    /*
    * 删除一个点的所有关系
    * */
    public void deleteRelationship(String startId,DirectionEnum direction){
        StringBuffer cypher = new StringBuffer();
        cypher.append("MATCH (n {id:'"+startId+"'})");
        if(direction== DirectionEnum.OUTGOING){
            cypher.append("-[r]->");
        }else if(direction== DirectionEnum.INCOMING){
            cypher.append("<-[r]-");
        }else{
            cypher.append("-[r]-");
        }
        cypher.append("() DELETE r");
        run(cypher.toString());
    }

    /*
    * 删除指定点以及所有有关系的点
    * */
    public void deletePath(String startId, DirectionEnum direction){
        StringBuffer cypher = new StringBuffer();
        cypher.append("MATCH data=");
        cypher.append("(n: {id:'" + startId + "'}) ");
        cypher.append("--");
        cypher.append("(n) ");
        cypher.append("RETURN data");
        run(cypher.toString());
    }

    /*
    * 查询一个节点的关联树
    * "match data=(root:ServiceNode{name:'UI'})-[*1..3]->(n) return data"
    * */
    public Path queryRelationshipPathNode(Node startNode, DirectionEnum direction, int deep) {
        StringBuffer query = new StringBuffer();
        query.append("MATCH data=");
        query.append("(root:" + startNode.getClass().getSimpleName() + "{id:'" + startNode.getId().toString() + "'}) ");
        query.append(configRelationshipDeepScript(direction, deep));
        query.append("(n) ");
        query.append("RETURN data");
        StatementResult result = run(query.toString(), Collections.emptyMap());
        return statementResultToPath(result);
    }

    /*
    * 查询两点间最短路径
    * MATCH (a:ServiceNode {id:'8bd4db68-f6bf-4c5d-8099-97c496f78a1c'}),(b:ServiceNode{id:'eea0a8f7-575d-47bc-8e5f-f160e74546c5'}),data=shortestpath((a)-[*..10]->(b))RETURN data
    * */
    public Path queryShortPath(Node startNode, Node endNode, DirectionEnum direction, int deep) {
        StringBuffer query = new StringBuffer();
        try {
            query.append("MATCH (a:" + startNode.getClass().getSimpleName() + " {id:'" + startNode.getId().toString() + "'}),");
            query.append("(b:" + endNode.getClass().getSimpleName() + "{id:'" + endNode.getId().toString() + "'}),");
            query.append("data=shortestpath((a)" + configRelationshipDeepScript(direction, deep) + "(b))");
            query.append("RETURN data");
            StatementResult result = run(query.toString(), Collections.emptyMap());
            return statementResultToPath(result);
        } catch (Exception e) {
            LOG.error(query.toString(), e);
        }
        return null;
    }

    /*
    * 查询关联的节点
    * MATCH(:ServiceNode { id: 'b6536c4d-99eb-45d2-a31f-a38be43e76fa' })--(n) RETURN n
    * */
    public List<Node> queryRelationshipNode(Node node, DirectionEnum direction) {
        StringBuffer query = new StringBuffer();
        try {
            query.append("MATCH ");
            query.append("(:" + node.getClass().getSimpleName() + " { id: '" + node.getId().toString() + "' })");
            query.append(configRelationshipScript(direction));
            query.append("(n) RETURN n");
            List<Node> data = new ArrayList<Node>();
            StatementResult result = run(query.toString(), Collections.emptyMap());
            while (result.hasNext()) {
                Record record = result.next();
                InternalNode internalNode = (InternalNode) record.get("n").asNode();
                Node tmpnode = new Node();
                tmpnode.setId(internalNode.get("id").asString());
                tmpnode.setName(internalNode.get("name").asString());
                tmpnode.setLabels(String.join(":", internalNode.labels()));
                tmpnode.setProperites(internalNode.asMap());
                data.add(tmpnode);
            }
            return data;
        } catch (Exception e) {
            LOG.error("查询失败 " + query, e);
            throw e;
        }
    }

    /*
    * 导入关系
    * MATCH (n:CMDB_BUSI_INS {id:'CMDB_BUSI_INS-3967516587'})
    * MATCH (m:CMDB_REDIS_CLUSTER {id:'CMDB_REDIS_CLUSTER-2479758977'})
    * CREATE (n)-[r:gift]->(m) return r;
    * */
    public void importRelationshipCSV(File file, String id_col, String type, String start_id_col, String end_id_col) throws Exception {
        Reader in = new FileReader(file);
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);
        for (CSVRecord record : records) {
            Relationship relationship = new Relationship();
            Iterator<String> it = record.toMap().keySet().iterator();
            while (it.hasNext()) {
                String name = it.next();
                if (name.equalsIgnoreCase(id_col)) {
                    relationship.setId(record.get(name));
                } else if (name.equalsIgnoreCase(start_id_col)) {
                    relationship.setStartId(record.get(name));
                } else if (name.equalsIgnoreCase(end_id_col)) {
                    relationship.setEndId(record.get(name));
                } else if (name.equalsIgnoreCase(type)) {
                    relationship.setType(record.get(name));
                } else {
                    relationship.getProperites().put(name, record.get(name));
                }
            }
            StringBuffer cypher = new StringBuffer();
            cypher.append("MATCH (n {id:'" + relationship.getStartId() + "'}) ");
            cypher.append("MATCH (m {id:'" + relationship.getEndId() + "'}) ");

            StringBuffer properite = new StringBuffer();
            properite.append("{");
            properite.append("id:'" + relationship.getId() + "'");
            Iterator<String> i = relationship.getProperites().keySet().iterator();
            while (i.hasNext()) {
                String key = i.next();
                Object value = relationship.getProperites().get(key);
                if (value instanceof String) {
                    properite.append("," + key + ":'" + value + "'");
                } else if (value instanceof Number) {
                    properite.append("," + key + ":" + value);
                } else {
                    properite.append("," + key + ":'" + value + "'");
                }
            }
            properite.append("}");

            cypher.append("CREATE (n)-[r:" + relationship.getType() + properite.toString() + "]->(m) RETURN r");
            run(cypher.toString(), Collections.emptyMap());
        }
    }

    //导入node
    public void importNodeCSV(File file, String id_col, String name_col, String type_col) throws Exception {
        Reader in = new FileReader(file);
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);
        for (CSVRecord record : records) {
            Node node = new Node();
            Iterator<String> it = record.toMap().keySet().iterator();
            while (it.hasNext()) {
                String name = it.next();
                if (name.equalsIgnoreCase(id_col)) {
                    node.setId(record.get(name));
                } else if (name.equalsIgnoreCase(name_col)) {
                    node.setName(record.get(name));
                } else if (name.equalsIgnoreCase(type_col)) {
                    node.setLabels(record.get(name));
                } else {
                    node.getProperites().put(name, record.get(name));
                }
            }
            StringBuffer cypher = new StringBuffer();
            cypher.append("CREATE(");
            cypher.append("n:" + node.getLabels());
            cypher.append(node.getProperitesJSON());
            cypher.append(")");
            run(cypher.toString(), Collections.emptyMap());
        }
    }

    public Session getSession() {
        return neo4jSessionFactory.getNeo4jSession();
    }

    public Transaction getTransaction() {
        return transactions.get();
    }

    private String configRelationshipScript(DirectionEnum direction) {
        if (direction == DirectionEnum.UNDIRECTED) {
            return "--\n";
        } else if (direction == DirectionEnum.INCOMING) {
            return "<--\n";
        } else if (direction == DirectionEnum.OUTGOING) {
            return "-->\n";
        } else {
            LOG.error("不支持的参数direction={}", direction);
            return "--\n";
        }

    }

    private String configRelationshipDeepScript(DirectionEnum direction, int deep) {
        if (direction == DirectionEnum.UNDIRECTED) {
            return "-[*.." + deep + "]-\n";
        } else if (direction == DirectionEnum.INCOMING) {
            return "<-[*.." + deep + "]-\n";
        } else if (direction == DirectionEnum.OUTGOING) {
            return "-[*.." + deep + "]->\n";
        } else {
            LOG.error("不支持的参数direction={}", direction);
            return "--\n";
        }
    }

    private Path statementResultToPath(StatementResult result){
        Path path = new Path();
        List<Node> nodes = new ArrayList<Node>();
        List<Relationship> relationships = new ArrayList<Relationship>();
        Map<Long, String> nodeCache = new HashMap<>();
        Map<Long, String> relationshipCache = new HashMap<>();
        while (result.hasNext()) {
            Record record = result.next();
            InternalPath internalPath = (InternalPath) record.get("data").asPath();
            //nodes
            Iterator<org.neo4j.driver.v1.types.Node> nodeit = internalPath.nodes().iterator();
            while (nodeit.hasNext()) {
                org.neo4j.driver.v1.types.Node node = nodeit.next();
                if (!nodeCache.containsKey(node.id())) {
                    Node tmpnode = new Node();
                    tmpnode.setId(node.get("id").asString());
                    tmpnode.setName(node.get("name").asString());
                    tmpnode.setLabels(String.join(":", node.labels()));
                    tmpnode.setProperites(node.asMap());
                    nodes.add(tmpnode);
                    nodeCache.put(node.id(), tmpnode.getId());
                }
            }
            //relations
            Iterator<org.neo4j.driver.v1.types.Relationship> relait = internalPath.relationships().iterator();
            while (relait.hasNext()) {
                org.neo4j.driver.v1.types.Relationship rela = relait.next();
                if (!relationshipCache.containsKey(rela.id())) {
                    Relationship obj = new Relationship();
                    obj.setId(rela.get("id").asString());
                    obj.setStartId(nodeCache.get(rela.startNodeId()));
                    obj.setEndId(nodeCache.get(rela.endNodeId()));
                    obj.setType(rela.type());
                    obj.setProperites(rela.asMap());
                    relationshipCache.put(rela.id(), rela.get("id").asString());
                    relationships.add(obj);
                }
            }
        }
        path.setNodes(nodes);
        path.setRelationships(relationships);
        return path;
    }

}
