package mongo;

import java.util.ArrayList;

import org.bson.Document;

import random.RanGen;

public class QueryFactory {

	public static void main(String args[]) throws Exception {

	}

	RanGen ran_group;
	RanGen ran_sum;

	ArrayList<String> groupBy = new ArrayList<String>();
	ArrayList<String> sum = new ArrayList<String>();

	String groupBySelected1;
	String groupBySelected2;
	String groupBySelected3;

	public QueryFactory() {

		// populate groupby

		groupBy.add("FundCode");
		groupBy.add("BookCode");
		groupBy.add("ParentFundCode");

		groupBy.add("AssetClassCode");
		groupBy.add("InvestmentTypeCode");
		groupBy.add("LocalCurrencyCode");

		groupBy.add("CompanyCode");
		groupBy.add("IssueCountryCode");
		groupBy.add("IndustryGroupCode");

		groupBy.add("MarketCapCode");
		groupBy.add("SecurityCode");

		// populate sum

		sum.add("Exposure");
		sum.add("GrossBaseDTD");
		sum.add("BookGrossBaseDTD");
		sum.add("LongExposure");
		sum.add("ShortExposure");
		sum.add("NetExposure");
		sum.add("GrossExposure");
		sum.add("marketvaluebase");

		// random init

		ran_group = new RanGen(0, groupBy.size() - 1);

		groupBySelected1 = getRandomGroupBy();
		groupBySelected2 = getRandomGroupBy();
		groupBySelected3 = getRandomGroupBy();

	}

	private String getRandomGroupBy() {

		return groupBy.get(ran_group.get());

	}

	public Document genrateQuery() {

		Document claues = new Document();

		Document where = new Document();

		where.append(groupBySelected1, "$" + groupBySelected1);
		where.append(groupBySelected2, "$" + groupBySelected2);
		where.append(groupBySelected3, "$" + groupBySelected3);

		claues.append("_id", where);

		for (String temp1 : sum) {
			claues.append(temp1, new Document("$sum", "$" + temp1));
		}

		Document group = new Document("$group", claues);

		return group;
	}

	public String getGroupby() {
		return groupBySelected1 + "-" + groupBySelected2 + "-" + groupBySelected3;
	}

}
