package com.boom.marketUpdate.adr.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.log4j.Logger;

public final class ConnectionValidator {

	/* declaration of JDBC Connection Validator */

	final static Logger logger = Logger.getLogger(ConnectionValidator.class);

	public ConnectionValidator() {

	}

	/* Check the connection is valid or not */
	public boolean isValid(Connection con) {
		if (con == null) {
			logger.error("No Connection was found");
			return false;
		}
		try {
			logger.debug("Connection has been created");
			return !con.isClosed();
		} catch (SQLException se) {
			logger.error(se);
			return false;
		}
	}

	/* Close the connection */
	public void invalidate(Connection con) {

		try {
			logger.info("Finished and Closed connection");
			con.close();
		} catch (SQLException se) {
			logger.error(se);
		}
	}
}