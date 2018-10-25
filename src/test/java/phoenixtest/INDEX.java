package phoenixtest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class INDEX {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		// String connString = "jdbc:phoenix:localhost:2181";
		String connString = "jdbc:phoenix:ip-10-0-20-140.eu-west-1.compute.internal,ip-10-0-1-89.eu-west-1.compute.internal,ip-10-0-30-54.eu-west-1.compute.internal:2181:/hbase-unsecure";

		Connection con = DriverManager.getConnection(connString);

		Statement stmt = con.createStatement();

		stmt.executeUpdate("CREATE INDEX fxindex ON nezu_pos_2 (LocalCurrencyCode)");
		con.commit();

		stmt.executeUpdate("CREATE INDEX priceindex ON nezu_pos_2 (SecurityCode)");
		con.commit();

		stmt.executeUpdate("CREATE INDEX tradeindex ON nezu_pos_2 (PositionKey)");
		con.commit();

		con.close();

	}

}
