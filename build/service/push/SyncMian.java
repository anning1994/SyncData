package cc.gukeer.syncdata.service.push;

import cc.gukeer.common.utils.PropertiesUtil;
import cc.gukeer.open.persistence.dao.AppMapper;
import cc.gukeer.open.persistence.dao.RefPlatformAppMapper;
import cc.gukeer.open.persistence.entity.App;
import cc.gukeer.open.persistence.entity.AppExample;
import cc.gukeer.open.persistence.entity.RefPlatformApp;
import cc.gukeer.open.persistence.entity.RefPlatformAppExample;
import cc.gukeer.syncdata.dataDefinition.EventData;
import cc.gukeer.syncdata.dataDefinition.EventType;
import cc.gukeer.syncdata.persistence.dao.SyncBaseMapper;
import cc.gukeer.syncdata.util.LineToCameUtil;
import cc.gukeer.syncdata.util.MD5Util;
import com.google.gson.Gson;
import org.apache.commons.lang.RandomStringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessagePostProcessor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by lx on 2017/2/7.
 */
@Service
public class SyncMian {
    @Autowired
    SyncBaseMapper syncBaseMapper;
    @Autowired
    AppMapper appMapper;
    @Autowired
    RefPlatformAppMapper refPlatformAppMapper;

    //入口方法
    //需要加事务
    public void execute() {
        //schema必须配置正确
        Properties properties = PropertiesUtil.getProperties("/syncdata.properties");
        //数据库名称
        String schema = properties.getProperty("sync.jdbc.schema");
        //生成表前缀
        String _tableNamePrefix = properties.getProperty("sync.mark.table.prefix");

        Set<List<RefPlatformApp>> parms = getAppPushParms(null);
        Iterator<List<RefPlatformApp>> iterator = parms.iterator();
        while (iterator.hasNext()) {
            List<RefPlatformApp> appParms = iterator.next();
            for (RefPlatformApp appParm : appParms) {
                String _mark = appParm.getQueues();
                String mark = _mark + ",";
                String appId = appParm.getAppId();
                String notLikeMark = "%" + _mark + ",%";
                //同步表查询条件
                String tableNamePrefix = _tableNamePrefix + "%";
                List<String> tableNames = getTableNames(schema, tableNamePrefix);
                for (String tableName : tableNames) {
                    //查询数据
                    List<Map<String, Object>> datas = getDatas(tableName, notLikeMark);
                    //获取数据的id集合
                    List<String> ids = new ArrayList<>();
                    for (Map<String, Object> data : datas) {
                        ids.add(data.get("id").toString());
                    }
                    if (datas.size() == 0) {
                        continue;
                    }
                    //序列化数据
                    Map<String, Object> datasJson = formatDatas(datas, tableName);

                    //推送数据
                    try {
                        if (!datasJson.get("insert").equals("false")) {
                            pushdata(appId,datasJson.get("insert"), appParm.getQueues(), "High");
                        }
                        if (!datasJson.get("modify").equals("false")) {
                            pushdata(appId,datasJson.get("modify"), appParm.getQueues(), "normal");
                        }
                        if (!datasJson.get("delete").equals("false")) {
                            pushdata(appId,datasJson.get("delete"), appParm.getQueues(), "normal");
                        }

                    } catch (Exception e) {
                        //如果失败，则不标记同步过的数据，直接返回
                        break;
                    }
                    //标记已经同步过的数据
                    try {
                        identityDeletion(tableName, mark, ids, notLikeMark);
                    } catch (Exception e) {

                    }
                }
            }
        }
    }
    //初始化时使用
    public void init(String id){
        //schema必须配置正确
        Properties properties = PropertiesUtil.getProperties("/syncdata.properties");
        //数据库名称
        String schema = properties.getProperty("sync.jdbc.schema");
        //生成表前缀
        String _tableNamePrefix = properties.getProperty("sync.mark.table.prefix");

        Set<List<RefPlatformApp>> parms = getAppPushParms(id);
        Iterator<List<RefPlatformApp>> iterator = parms.iterator();
        while (iterator.hasNext()) {
            List<RefPlatformApp> appParms = iterator.next();
            for (RefPlatformApp appParm : appParms) {
                String _mark = appParm.getQueues();
                String appId = appParm.getAppId();
                String mark = _mark + ",";
                String notLikeMark = "%" + _mark + ",%";
                //同步表查询条件
                String tableNamePrefix = _tableNamePrefix + "%";
                List<String> tableNames = getTableNames(schema, tableNamePrefix);
                for (String tableName : tableNames) {
                    //查询数据
                    List<Map<String, Object>> datas = getDatas(tableName, notLikeMark);
                    //获取数据的id集合
                    List<String> ids = new ArrayList<>();
                    for (Map<String, Object> data : datas) {
                        ids.add(data.get("id").toString());
                    }
                    if (datas.size() == 0) {
                        continue;
                    }
                    //序列化数据
                    Map<String, Object> datasJson = formatDatas(datas, tableName);

                    //推送数据
                    try {
                        if (!datasJson.get("insert").equals("false")) {
                            pushdata(appId,datasJson.get("insert"), appParm.getQueues(), "High");
                        }
                        if (!datasJson.get("modify").equals("false")) {
                            pushdata(appId,datasJson.get("modify"), appParm.getQueues(), "normal");
                        }
                        if (!datasJson.get("delete").equals("false")) {
                            pushdata(appId,datasJson.get("delete"), appParm.getQueues(), "normal");
                        }

                    } catch (Exception e) {
                        //如果失败，则不标记同步过的数据，直接返回
                        break;
                    }
                    //标记已经同步过的数据
                    try {
                        identityDeletion(tableName, mark, ids, notLikeMark);
                    } catch (Exception e) {

                    }
                }
            }
        }
    }

    //获取需要同步的表名
    public List<String> getTableNames(String schema, String tableNamePrefix) {
        List<String> tableNames = syncBaseMapper.getTableNames(schema, tableNamePrefix);
        return tableNames;
    }

    //根据表名获取数据
    public List<Map<String, Object>> getDatas(String tableName, String notLikeMark) {
        List<Map<String, Object>> _datas = syncBaseMapper.getDatas(tableName, notLikeMark);
        List<Map<String, Object>> datas = new ArrayList<>();
        for (Map<String, Object> data : _datas) {
            Map<String, Object> columnMap = new HashMap<>();
            Set<String> keySet = data.keySet();
            Iterator<String> keyIterator = keySet.iterator();
            while (keyIterator.hasNext()) {
                String _column = keyIterator.next();
                String column = LineToCameUtil.underlineToCamel2(_column);
                columnMap.put(column, data.get(_column));
            }
            datas.add(columnMap);
        }
        return datas;
    }

    //序列化数据
    public Map<String, Object> formatDatas(List<Map<String, Object>> datas, String tableName) {
        //封装插入数据
        List<Map<String, Object>> insertDatas = new ArrayList<>();
        //封装修改数据
        List<Map<String, Object>> modifyDatas = new ArrayList<>();
        //封装删除数据
        List<Map<String, Object>> deleteDatas = new ArrayList<>();

        //将datas按照事件拆分为三种不同的list
        for (Map<String, Object> data : datas) {
            data.remove("source");
            data.remove("syncDelFlag");
            data.remove("syncDate");
            if (data.get("event").equals(EventType.INSERT.name())) {
                data.remove("event");
                insertDatas.add(data);
                continue;
            }
            if (data.get("event").equals(EventType.MODIFY.name())) {
                data.remove("event");
                modifyDatas.add(data);
                continue;
            }
            if (data.get("event").equals(EventType.DELETE.name())) {
                data.remove("event");
                deleteDatas.add(data);
                continue;
            }
        }
        Map<String, Object> datasJson = new HashMap<>();
        //封装插入数据
        if (insertDatas.size() != 0) {
            EventData insertEventData = new EventData();
            insertEventData.setDataList(insertDatas);
            insertEventData.setObjectKey(tableName);
            insertEventData.setEvent(EventType.INSERT);
            String insertDatasJson = eventDataToJson(insertEventData);
            datasJson.put("insert", insertDatasJson);
        } else {
            datasJson.put("insert", "false");
        }
        //封装修改数据
        if (modifyDatas.size() != 0) {
            EventData modifyEventData = new EventData();
            modifyEventData.setDataList(modifyDatas);
            modifyEventData.setObjectKey(tableName);
            modifyEventData.setEvent(EventType.MODIFY);
            String modifyDatasJson = eventDataToJson(modifyEventData);
            datasJson.put("modify", modifyDatasJson);
        } else {
            datasJson.put("modify", "false");
        }

        // 封装删除数据
        if (deleteDatas.size() != 0) {
            EventData deleteEventData = new EventData();
            deleteEventData.setDataList(deleteDatas);
            deleteEventData.setObjectKey(tableName);
            deleteEventData.setEvent(EventType.DELETE);
            String deleteDatasJson = eventDataToJson(deleteEventData);
            datasJson.put("delete", deleteDatasJson);
        } else {
            datasJson.put("delete", "false");
        }
        return datasJson;
    }

    //转json
    public String eventDataToJson(EventData eventData) {
       /* Gson gson = new GsonBuilder().disableHtmlEscaping().create();*/
        Gson gson = new Gson();
        String datasJson = gson.toJson(eventData);
        return datasJson;
    }

    //标记已经同步过的数据
    public void identityDeletion(String tableName, String mark, List<String> ids, String notLikeMark) {
        syncBaseMapper.identityDeletion(tableName, mark, ids, notLikeMark);
    }

    //转化时间
    public String makeTime(Long time) {
        String format = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat fmt = new SimpleDateFormat(format);
        String timeString = fmt.format(time);
        return timeString;
    }

    //推送数据
    public String pushdata(String appId,Object messageContent, String queuesName, String Priority) {
        App app = appMapper.selectByPrimaryKey(appId);
        String AGENT =queuesName;
        String ACCESSKEY = app.getAppSecret();
        String property = "userId:1";
        Map<String, Object> generateSignParams = new HashMap<String, Object>();
        generateSignParams.put("agent", AGENT);
        generateSignParams.put("accesskey", ACCESSKEY);
        generateSignParams.put("rand", RandomStringUtils.randomAlphanumeric(8));
        generateSignParams.put("time", new Date().getTime());
        generateSignParams.put("data", messageContent);

        //生成签名
        String sign = MD5Util.go(AGENT + ACCESSKEY + generateSignParams.get("rand").toString() + messageContent.toString() + generateSignParams.get("time").toString());
        System.out.println("签名是+++++++++++" + sign);
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("rand", generateSignParams.get("rand"));
        params.put("sign", sign);
        params.put("time", generateSignParams.get("time"));
        params.put("property", property);
        params.put("agent", generateSignParams.get("agent"));
        params.put("datas", messageContent);

        if (Priority.equals("High")) {
            sendPriorityHigh(params, queuesName);
        } else {
            send(params, queuesName);
        }
        return null;
    }

    @Resource(name = "jmsTemplate")
    private JmsTemplate jmsTemplate;

    public void sendPriorityHigh(final Map<String, Object> message, String queuesName) {
        jmsTemplate.convertAndSend(queuesName, message, new MessagePostProcessor() {
            @Override
            public Message postProcessMessage(Message message) throws JMSException {
                message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
                message.setJMSCorrelationID("gkpltaform");
                message.setJMSPriority(10);
                return message;
            }
        });
    }

    public void send(final Map<String, Object> message, String queuesName) {
        jmsTemplate.convertAndSend(queuesName, message, new MessagePostProcessor() {

            @Override
            public Message postProcessMessage(Message message) throws JMSException {
                message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
                message.setJMSCorrelationID("gkpltaform");
                message.setJMSPriority(10);
                return message;
            }
        });
    }

    public Set<List<RefPlatformApp>> getAppPushParms(String id) {
        Set<List<RefPlatformApp>> parms = new HashSet<>();
        RefPlatformAppExample refPlatExample = new RefPlatformAppExample();
        List<Integer> statusList = new ArrayList<>();
        statusList.add(1);
        statusList.add(2);
        if (id!=null) {
            refPlatExample.createCriteria().andIdEqualTo(id);
        }else {
            refPlatExample.createCriteria().andAppStatusIn(statusList).andOptStatusEqualTo(1).andSyncStatusEqualTo(1).andDataStatusEqualTo(1);
        }
        List<RefPlatformApp> refPlatformApps = refPlatformAppMapper.selectByExample(refPlatExample);
        parms.add(refPlatformApps);
        return parms;
}

}
