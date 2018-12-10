package cn.itcast.util;

import java.io.Serializable;

/**
 * 
 * ClassName: BinlogValue <br/> 
 * 
 * binlog分析的每行每列的value值；<br>
 * 新增数据：beforeValue 和 value 均为现有值；<br>
 * 修改数据：beforeValue是修改前的值；value为修改后的值；<br>
 * 删除数据：beforeValue和value均是删除前的值； 这个比较特殊主要是为了删除数据时方便获取删除前的值<br>
 * 
 * @author Deng
 */
public class BinlogValue implements Serializable {

	private static final long serialVersionUID = -6350345408773943086L;
	
	private String value;
	private String beforeValue;
	
	/**
	 * binlog分析的每行每列的value值；<br>
	 * 新增数据： value：为现有值；<br>
	 * 修改数据：value为修改后的值；<br>
	 * 删除数据：value是删除前的值； 这个比较特殊主要是为了删除数据时方便获取删除前的值<br>
	 */
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
	/**
	 * binlog分析的每行每列的beforeValue值；<br>
	 * 新增数据：beforeValue为现有值；<br>
	 * 修改数据：beforeValue是修改前的值；<br>
	 * 删除数据：beforeValue为删除前的值； <br>
	 *
	 */
	public String getBeforeValue() {
		return beforeValue;
	}
	public void setBeforeValue(String beforeValue) {
		this.beforeValue = beforeValue;
	}
	
	

}
