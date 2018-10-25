package mongo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import random.RanGen;

public class MongoFactory {

	private MongoCRUD crud;

	private ArrayList<String> distinctFX = new ArrayList<String>();
	private ArrayList<String> distinctSecurity = new ArrayList<String>();
	private ArrayList<String> distinctPosition = new ArrayList<String>();

	private RanGen ran_distinctFX;
	private RanGen ran_distinctSecurity;
	private RanGen ran_distinctPosition;

	public MongoFactory(String Host, int port) {

		System.out.println("hrrrr");

		crud = new MongoCRUD(Host, port);

		distinctFX = getDistinctItems("LocalCurrencyCode");
		distinctSecurity = getDistinctItems("SecurityCode");
		distinctPosition = getDistinctItems("PositionKey");

		ran_distinctFX = new RanGen(1, distinctFX.size() - 1);
		ran_distinctSecurity = new RanGen(1, distinctSecurity.size() - 1);
		ran_distinctPosition = new RanGen(1, distinctPosition.size() - 1);

	}

	private ArrayList<String> getDistinctItems(String item) {

		ArrayList<HashMap<String, Object>> t = crud.getDocs("nezu_data_db", "nezu_position_denorm_data_coll_new");

		HashMap<String, Integer> distinctItem = new HashMap<String, Integer>();

		for (HashMap<String, Object> temp : t) {

			distinctItem.put(temp.get(item).toString(), 1);

		}

		Set<String> m = distinctItem.keySet();

		ArrayList<String> temp = new ArrayList<String>(m);

		return temp;
	}

	public String getRandomFx() {
		return distinctFX.get(ran_distinctFX.get());
	}

	public String getRandomSecurity() {
		return distinctSecurity.get(ran_distinctSecurity.get());
	}

	public String getRandomPosition() {
		return distinctPosition.get(ran_distinctPosition.get());
	}

}
