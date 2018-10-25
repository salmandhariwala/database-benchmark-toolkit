package phoenixtest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class DownloadDataMongo {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		// String connString = "jdbc:phoenix:localhost:2181";
		String connString = "jdbc:phoenix:ip-10-0-20-140.eu-west-1.compute.internal,ip-10-0-1-89.eu-west-1.compute.internal,ip-10-0-30-54.eu-west-1.compute.internal:2181:/hbase-unsecure";

		// createing table
		Statement stmt = null;

		Connection con = DriverManager.getConnection(connString);

		stmt = con.createStatement();
		stmt.executeUpdate(
				"create table nezu_pos_2(pk varchar primary key,MarketPriceBaseEOD INTEGER,Exposure INTEGER,MarketValueBaseEOD INTEGER,ISecID VARCHAR ,AsOfDate VARCHAR ,SecurityID VARCHAR ,PositionKey INTEGER,SecurityCode VARCHAR ,RootISecID VARCHAR ,FundCode VARCHAR ,BookCode VARCHAR ,StrategyCode VARCHAR ,PositionTypeCode VARCHAR ,ParentFundCode VARCHAR ,AssetClassCode VARCHAR ,InvestmentTypeCode VARCHAR ,LocalCurrencyCode VARCHAR ,CompanyCode VARCHAR ,IssueCountryCode VARCHAR ,IssueCurrencyCode VARCHAR ,IndustryGroupCode VARCHAR ,RootInvestmentTypeCode VARCHAR ,LongShort VARCHAR ,GrossBaseDTD INTEGER ,BookGrossBaseDTD INTEGER,LongExposure INTEGER,ShortExposure INTEGER,NetExposure INTEGER,GrossExposure INTEGER,MarketCapCode VARCHAR ,RootBBGTickerExchangeCode VARCHAR ,FXRateBase INTEGER,MarketPriceBase INTEGER,TradeQuantity INTEGER,marketvaluebase INTEGER)");
		con.commit();

		MongoClient client = new MongoClient("10.0.1.90", 10001);
		MongoDatabase database = client.getDatabase("nezu_data_db");
		MongoCollection<Document> collection = database.getCollection("nezu_position_denorm_data_coll_new");

		MongoCursor<Document> t = collection.find().iterator();

		int counter = 0;

		while (t.hasNext()) {
			counter++;
			Document temp = t.next();

			int MarketPriceBaseEOD = (int) temp.getDouble("MarketPriceBaseEOD").doubleValue();
			int Exposure = (int) temp.getDouble("Exposure").doubleValue();
			int MarketValueBaseEOD = (int) temp.getDouble("MarketValueBaseEOD").doubleValue();

			String ISecID = temp.getString("ISecID");
			String AsOfDate = temp.getDate("AsOfDate").toString();
			String SecurityID = temp.getString("SecurityID");

			int PositionKey = temp.getInteger("PositionKey");

			String SecurityCode = temp.getString("SecurityCode");
			String RootISecID = temp.getString("RootISecID");
			String FundCode = temp.getString("FundCode");
			String BookCode = temp.getString("BookCode");
			String StrategyCode = temp.getString("StrategyCode");
			String PositionTypeCode = temp.getString("PositionTypeCode");
			String ParentFundCode = temp.getString("ParentFundCode");
			String AssetClassCode = temp.getString("AssetClassCode");
			String InvestmentTypeCode = temp.getString("InvestmentTypeCode");
			String LocalCurrencyCode = temp.getString("LocalCurrencyCode");
			String CompanyCode = temp.getString("CompanyCode");
			String IssueCountryCode = temp.getString("IssueCountryCode");
			String IssueCurrencyCode = temp.getString("IssueCurrencyCode");
			String IndustryGroupCode = temp.getString("IndustryGroupCode");
			String RootInvestmentTypeCode = temp.getString("RootInvestmentTypeCode");
			String LongShort = temp.getString("LongShort");

			int GrossBaseDTD = (int) temp.getDouble("GrossBaseDTD").doubleValue();
			int BookGrossBaseDTD = (int) temp.getDouble("BookGrossBaseDTD").doubleValue();
			int LongExposure = (int) temp.getDouble("LongExposure").doubleValue();
			int ShortExposure = (int) temp.getDouble("ShortExposure").doubleValue();
			int NetExposure = (int) temp.getDouble("NetExposure").doubleValue();
			int GrossExposure = (int) temp.getDouble("GrossExposure").doubleValue();

			String RootBBGTickerExchangeCode = temp.getString("RootBBGTickerExchangeCode");
			String MarketCapCode = temp.getString("MarketCapCode");

			int FXRateBase;
			try {
				FXRateBase = temp.getInteger("FXRateBase");
			} catch (Exception e) {
				FXRateBase = (int) temp.getDouble("FXRateBase").doubleValue();
			}

			int MarketPriceBase;
			try {
				MarketPriceBase = temp.getInteger("MarketPriceBase");
			} catch (Exception e) {
				MarketPriceBase = (int) temp.getDouble("MarketPriceBase").doubleValue();
			}

			int TradeQuantity;
			try {
				TradeQuantity = temp.getInteger("TradeQuantity");
			} catch (Exception e) {
				TradeQuantity = (int) temp.getDouble("TradeQuantity").doubleValue();
			}

			int marketvaluebase = (int) temp.getDouble("marketvaluebase").doubleValue();

			try {

				String Query = "upsert into nezu_pos_2 values('" + counter + "'," + MarketPriceBaseEOD + "," + Exposure
						+ "," + MarketValueBaseEOD + ",'" + ISecID + "','" + AsOfDate + "','" + SecurityID + "',"
						+ PositionKey + ",'" + SecurityCode + "','" + RootISecID + "','" + FundCode + "','" + BookCode
						+ "','" + StrategyCode + "','" + PositionTypeCode + "','" + ParentFundCode + "','"
						+ AssetClassCode + "','" + InvestmentTypeCode + "','" + LocalCurrencyCode + "','" + CompanyCode
						+ "','" + IssueCountryCode + "','" + IssueCurrencyCode + "','" + IndustryGroupCode + "','"
						+ RootInvestmentTypeCode + "','" + LongShort + "'," + GrossBaseDTD + "," + BookGrossBaseDTD
						+ "," + LongExposure + "," + ShortExposure + "," + NetExposure + "," + GrossExposure + ",'"
						+ RootBBGTickerExchangeCode + "','" + MarketCapCode + "'," + FXRateBase + "," + MarketPriceBase
						+ "," + TradeQuantity + "," + marketvaluebase + ")";
				stmt.executeUpdate(Query);
				con.commit();
				
				System.out.println(Query);

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		con.close();
		client.close();

	}

}
