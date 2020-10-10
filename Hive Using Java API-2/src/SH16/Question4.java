package SH16;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Question4 {

	private static String driverClass = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverClass);
		} catch (ClassNotFoundException exception) {
			System.out.println(exception.toString());
			System.exit(1);
		}

		Connection connection = DriverManager.getConnection(
				"jdbc:hive2://localhost:10000/bks", "hive", "");

		Statement statement = connection.createStatement();

		// Create bucket table by country
		String createBucket = "create table countryBucket ("
				+ "athlete_name string, age int, country string, year int, closing_date string, sport string, gold int, silver int, bronze int, total int"
				+ ") clustered by (country) into 10 buckets"
				+ " row format delimited" + " fields terminated by ','";

		try {
			statement.execute(createBucket);
			System.out.println("Created bucket table.");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}


		// insert data into bucket table
		try {
			statement.execute("insert overwrite table countryBucket select * from olympic");
			System.out.println("Data loaded successfully");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		statement.close();
		connection.close();
	}
}