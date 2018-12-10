package cn.itcast.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.CanalEntry.TransactionBegin;
import com.alibaba.otter.canal.protocol.CanalEntry.TransactionEnd;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 
 * ClassName: CanalDataParser <br/> 
 * 
 * @author Deng
 */
public class CanalDataParser {
	
	protected static final String DATE_FORMAT 	= "yyyy-MM-dd HH:mm:ss";
	protected static final String yyyyMMddHHmmss = "yyyyMMddHHmmss";
	protected static final String yyyyMMdd 		= "yyyyMMdd";
	protected static final String SEP 			= SystemUtils.LINE_SEPARATOR;
	protected static String  context_format     = null;
    protected static String  row_format         = null;
    protected static String  transaction_format = null;
    protected static String row_log = null;
	
	private static Logger logger = LoggerFactory.getLogger(CanalDataParser.class);
	
	static {
        context_format = SEP + "****************************************************" + SEP;
        context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP;
        context_format += "* Start : [{}] " + SEP;
        context_format += "* End : [{}] " + SEP;
        context_format += "****************************************************" + SEP;

        row_format = SEP
                     + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms"
                     + SEP;

        transaction_format = SEP + "================> binlog[{}:{}] , executeTime : {} , delay : {}ms" + SEP;

        row_log = "schema[{}], table[{}]";
    }



	public static List<InnerBinlogEntry> convertToInnerBinlogEntry(Message message) {
		List<InnerBinlogEntry> innerBinlogEntryList = new ArrayList<InnerBinlogEntry>();
		
		if(message == null) {
			logger.info("接收到空的 message; 忽略");
			return innerBinlogEntryList;
		}
		
		long batchId = message.getId();
        int size = message.getEntries().size();
        if (batchId == -1 || size == 0) {
        	logger.info("接收到空的message[size=" + size + "]; 忽略");
        	return innerBinlogEntryList;
        }

        printLog(message, batchId, size);
        List<Entry> entrys = message.getEntries();

        //输出日志
        for (Entry entry : entrys) {
        	long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;
        	
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN) {
                    TransactionBegin begin = null;
                    try {
                        begin = TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    logger.info("BEGIN ----> Thread id: {}",  begin.getThreadId());
                    logger.info(transaction_format, new Object[] {entry.getHeader().getLogfileName(),
                                String.valueOf(entry.getHeader().getLogfileOffset()), String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime) });

                } else if (entry.getEntryType() == EntryType.TRANSACTIONEND) {
                    TransactionEnd end = null;
                    try {
                        end = TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    logger.info("END ----> transaction id: {}", end.getTransactionId());
                    logger.info(transaction_format,
                        new Object[] {entry.getHeader().getLogfileName(),  String.valueOf(entry.getHeader().getLogfileOffset()),
                                String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime) });
                }
                continue;
            }


            //解析结果
            if (entry.getEntryType() == EntryType.ROWDATA) {
                RowChange rowChage = null;
                try {
                    rowChage = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                EventType eventType = rowChage.getEventType();

                logger.info(row_format, new Object[] { entry.getHeader().getLogfileName(),
                            String.valueOf(entry.getHeader().getLogfileOffset()), entry.getHeader().getSchemaName(),
                            entry.getHeader().getTableName(), eventType, String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime) });


                //组装数据结果
                if (eventType == EventType.INSERT || eventType == EventType.DELETE || eventType == EventType.UPDATE) {
                	String schemaName = entry.getHeader().getSchemaName();
                	String tableName = entry.getHeader().getTableName();
                	List<Map<String, BinlogValue>> rows = parseEntry(entry);

                	InnerBinlogEntry innerBinlogEntry = new InnerBinlogEntry();
                	innerBinlogEntry.setEntry(entry);
                	innerBinlogEntry.setEventType(eventType);
                	innerBinlogEntry.setSchemaName(schemaName);
                	innerBinlogEntry.setTableName(tableName.toLowerCase());
                	innerBinlogEntry.setRows(rows);

                	innerBinlogEntryList.add(innerBinlogEntry);
                } else {
                	logger.info(" 存在 INSERT INSERT UPDATE 操作之外的SQL [" + eventType.toString() + "]");
                }
                continue;
            }
        }
		
		return innerBinlogEntryList;
	}





	private static List<Map<String, BinlogValue>> parseEntry(Entry entry) {
		List<Map<String, BinlogValue>> rows = new ArrayList<Map<String, BinlogValue>>();
		try {
			String schemaName = entry.getHeader().getSchemaName();
        	String tableName = entry.getHeader().getTableName();
			RowChange rowChage = RowChange.parseFrom(entry.getStoreValue());
			EventType eventType = rowChage.getEventType();

			// 处理每个Entry中的每行数据
			for (RowData rowData : rowChage.getRowDatasList()) {
				StringBuilder rowlog = new StringBuilder("rowlog schema[" + schemaName + "], table[" + tableName + "], event[" + eventType.toString() + "]");
				
				Map<String, BinlogValue> row = new HashMap<String, BinlogValue>();
				List<Column> beforeColumns = rowData.getBeforeColumnsList();
				List<Column> afterColumns = rowData.getAfterColumnsList();
				beforeColumns = rowData.getBeforeColumnsList();
			    if (eventType == EventType.DELETE) {//delete
			    	for(Column column : beforeColumns) {
			    		BinlogValue binlogValue = new BinlogValue();
			    		binlogValue.setValue(column.getValue());
			    		binlogValue.setBeforeValue(column.getValue());
				    	row.put(column.getName(), binlogValue);
				    }
			    } else if(eventType == EventType.UPDATE) {//update
			    	for(Column column : beforeColumns) {
			    		BinlogValue binlogValue = new BinlogValue();
			    		binlogValue.setBeforeValue(column.getValue());
				    	row.put(column.getName(), binlogValue);
				    }
			    	for(Column column : afterColumns) {
			    		BinlogValue binlogValue = row.get(column.getName());
			    		if(binlogValue == null) {
			    			binlogValue = new BinlogValue();
			    		}
			    		binlogValue.setValue(column.getValue());
				    	row.put(column.getName(), binlogValue);
				    }
			    } else { // insert
			    	for(Column column : afterColumns) {
			    		BinlogValue binlogValue = new BinlogValue();
			    		binlogValue.setValue(column.getValue());
			    		binlogValue.setBeforeValue(column.getValue());
				    	row.put(column.getName(), binlogValue);
				    }
			    } 
			    
			    rows.add(row);
			    String rowjson = JacksonUtil.obj2str(row);
			    
			    logger.info("########################### Data Parse Result ###########################");
			    logger.info(rowlog + " , " + rowjson);
			    logger.info("########################### Data Parse Result ###########################");
			    logger.info("");
			}
		} catch (InvalidProtocolBufferException e) {
			throw new RuntimeException("parseEntry has an error , data:" + entry.toString(), e);
		}
        return rows;
	}





	private static void printLog(Message message, long batchId, int size) {
        long memsize = 0;
        for (Entry entry : message.getEntries()) {
            memsize += entry.getHeader().getEventLength();
        }

        String startPosition = null;
        String endPosition = null;
        if (!CollectionUtils.isEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
        }

        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        logger.info(context_format, new Object[] {batchId, size, memsize, format.format(new Date()), startPosition, endPosition });
    }



	private static String buildPositionForDump(Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        return entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":" + entry.getHeader().getExecuteTime() + "(" + format.format(date) + ")";
    }
	
}
