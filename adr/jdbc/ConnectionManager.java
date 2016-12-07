package com.boom.marketUpdate.adr.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.log4j.Logger;

import com.boom.util.sql.BoomDBDriver;

public class ConnectionManager {

	/* Connection declaration of JDBC */

	final static Logger logger = Logger.getLogger(ConnectionManager.class);

	/* Create new connection */
	public Connection createNew() {

		try {
			return DriverManager.getConnection(BoomDBDriver.URL_PREFIX + "market", "", "");
		} catch (SQLException se) {
			logger.error(se);
			throw new IllegalArgumentException("Unable to create new connection", se);
		}
	}

	public Connection getInstance(Connection con) {
		try {
			if (con.isClosed()) {
				return createNew();
			} else {
				return con;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
}
