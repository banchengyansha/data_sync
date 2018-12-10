package cn.itcast.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;

/**
 * 
 * ClassName: InnerBinlogEntry <br/> 
 * 
 * @author Deng
 */
public class InnerBinlogEntry {
	
	/**
	 * canal原生的Entry
	 */
	private Entry entry;
	
	/**
	 * 该Entry归属于的表名
	 */
	private String tableName;
	
	/**
	 * 该Entry归属数据库名
	 */
	private String schemaName;
	
	/**
	 * 该Entry本次的操作类型，对应canal原生的枚举；EventType.INSERT; EventType.UPDATE; EventType.DELETE;
	 */
	private EventType eventType;
	
	private List<Map<String, BinlogValue>> rows = new ArrayList<Map<String, BinlogValue>>();
	
	
	public Entry getEntry() {
		return entry;
	}
	public void setEntry(Entry entry) {
		this.entry = entry;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public EventType getEventType() {
		return eventType;
	}
	public void setEventType(EventType eventType) {
		this.eventType = eventType;
	}
	public String getSchemaName() {
		return schemaName;
	}
	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}
	public List<Map<String, BinlogValue>> getRows() {
		return rows;
	}
	public void setRows(List<Map<String, BinlogValue>> rows) {
		this.rows = rows;
	}

}
