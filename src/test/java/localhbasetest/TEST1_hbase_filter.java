package localhbasetest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class TEST1_hbase_filter {

	public static void main(String[] args) throws Exception {

		// Instantiating configuration class
		Configuration config;

		// Instantiating connection
		Connection conn;

		// Init admin
		Admin admin;

		config = HBaseConfiguration.create();
		conn = ConnectionFactory.createConnection(config);
		admin = conn.getAdmin();
		
		
		Table table = conn.getTable(TableName.valueOf("emp"));

		Get g = new Get(Bytes.toBytes("3"));

		Result result = table.get(g);
		
		byte [] value = result.getValue(Bytes.toBytes("personal data"),Bytes.toBytes("company"));
		
		String name = Bytes.toString(value);
		
		System.out.println(name);
	}

}
