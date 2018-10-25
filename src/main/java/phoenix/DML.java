package phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

public class DML {

	public static void main(String args[]) {
		boolean ENVIROMENT = false;

		DML dml = new DML(ENVIROMENT);

		System.out.println(dml.getkeys_LocalCurrencyCode("JpzxCSlFHTPOMQieoa3kMQ=="));
		System.out.println("******");
		System.out.println(dml.getkeys_PositionKey("84746"));
		System.out.println("******");
		System.out
				.println(dml.getkeys_SecurityCode("bEBwoxBQa97q9rPjmcWb49wpD7ILoHkamIIg5X/GYPnUeVHGIS6jg7AAPzvcvqyi"));

		dml.update_FXRateBase("10", 987654);
		dml.update_MarketPriceBase("10", 98765);
		dml.update_TradeQuantity("10", 876655);

		dml.update_marketvaluebase("10", 1111);
	}

	ResultSet rset;
	Properties props = new Properties();
	Connection con;
	Statement stmt;

	public DML(boolean isprod) {

		try {
			props.setProperty("phoenix.query.timeoutMs", "600000");
			props.setProperty("hbase.rpc.timeout", "1800000");

			if (!isprod) {
				con = DriverManager.getConnection("jdbc:phoenix:localhost:2181", props);
				stmt = con.createStatement();
			} else {
				con = DriverManager.getConnection(
						"jdbc:phoenix:ip-10-0-20-140.eu-west-1.compute.internal,ip-10-0-1-89.eu-west-1.compute.internal,ip-10-0-30-54.eu-west-1.compute.internal:2181:/hbase-unsecure",
						props);
				stmt = con.createStatement();
			}
		} catch (Exception e) {

		}

	}

	public ResultSet execQuery(String QUery) {

		try {
			PreparedStatement statement = con.prepareStatement(QUery);

			statement.setQueryTimeout(120);
			rset = statement.executeQuery();

			return rset;
		} catch (Exception e) {
			return null;
		}

	}

	public ArrayList<String> getkeys_LocalCurrencyCode(String where) {
		ArrayList<String> pk = new ArrayList<String>();
		try {
			PreparedStatement statement = con
					.prepareStatement("select * from nezu_pos_2 where LOCALCURRENCYCODE='" + where + "'");
			statement.setQueryTimeout(120);
			rset = statement.executeQuery();

			while (rset.next()) {

				pk.add(rset.getString("pk"));

			}

			return pk;
		} catch (Exception e) {
			return null;
		}

	}

	public ArrayList<String> getkeys_SecurityCode(String where) {
		ArrayList<String> pk = new ArrayList<String>();
		try {
			PreparedStatement statement = con
					.prepareStatement("select * from nezu_pos_2 where SecurityCode='" + where + "'");
			statement.setQueryTimeout(120);
			rset = statement.executeQuery();

			while (rset.next()) {

				pk.add(rset.getString("pk"));

			}

			return pk;
		} catch (Exception e) {
			return null;
		}

	}

	public ArrayList<String> getkeys_PositionKey(String where) {
		ArrayList<String> pk = new ArrayList<String>();
		try {
			PreparedStatement statement = con
					.prepareStatement("select * from nezu_pos_2 where PositionKey=" + where + "");
			statement.setQueryTimeout(120);
			rset = statement.executeQuery();

			while (rset.next()) {

				pk.add(rset.getString("pk"));

			}

			return pk;
		} catch (Exception e) {
			return null;
		}

	}

	public void update_FXRateBase(String pk, int value) {
		try {
			stmt.executeUpdate("upsert into nezu_pos_2(pk,FXRateBase) values ('" + pk + "'," + value + ")");
			con.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void update_MarketPriceBase(String pk, int value) {
		try {
			stmt.executeUpdate("upsert into nezu_pos_2(pk,MarketPriceBase) values ('" + pk + "'," + value + ")");
			con.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void update_TradeQuantity(String pk, int value) {
		try {
			stmt.executeUpdate("upsert into nezu_pos_2(pk,TradeQuantity) values ('" + pk + "'," + value + ")");
			con.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void update_marketvaluebase(String pk, int value) {
		try {
			stmt.executeUpdate("upsert into nezu_pos_2(pk,marketvaluebase) values ('" + pk + "'," + value + ")");
			con.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void close() {
		try {
			stmt.close();
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
