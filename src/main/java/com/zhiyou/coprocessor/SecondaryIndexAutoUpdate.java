package com.zhiyou.coprocessor;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;


//observer 的coprocessor 自动更新二级索引
//create 'order_item', 'i'
//create 'order_item_subtotal_index','r'
//协处理器 order_item, 索引自动更新到 order_item_subtotal_index
public class SecondaryIndexAutoUpdate extends BaseRegionObserver {
	//索引表数据添加
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
			throws IOException {
		
		
		List<Cell> subtotalCell = 	
			put.get(Bytes.toBytes("i"), Bytes.toBytes("subtotal"));
			
		if(subtotalCell != null && subtotalCell.size() > 0){
			
			RegionCoprocessorEnvironment environment = e.getEnvironment();
			Configuration configuration = environment.getConfiguration();
			
			//与hbase建立连接
			Connection connection = 
					ConnectionFactory.createConnection(configuration);
			Table table = 
					connection.getTable(
							TableName.valueOf(
									"bd14:order_item_subtotal_index"));
			
			//put对象
			Put indexPut = 
					new Put(CellUtil.cloneValue(subtotalCell.get(0)));
			indexPut.addColumn(Bytes.toBytes("r")
					, put.getRow()
					, null);
			table.put(indexPut);
			table.close();
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
