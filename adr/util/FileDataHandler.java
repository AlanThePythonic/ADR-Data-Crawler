package com.boom.marketUpdate.adr.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import com.boom.marketUpdate.adr.bean.CellBean;
import com.boom.marketUpdate.adr.bean.TupleBean;
import com.boom.marketUpdate.adr.bean.TupleBeanTriple;
import com.boom.marketUpdate.adr.jdbc.ConnectionManager;
import com.boom.marketUpdate.adr.jdbc.ConnectionValidator;
import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;

public class FileDataHandler {

	final static Logger logger = Logger.getLogger(FileDataHandler.class);
	private ArrayList<TupleBeanTriple<String, String, String>> finalNewList = new ArrayList<TupleBeanTriple<String, String, String>>();
	private ArrayList<TupleBeanTriple<String, String, String>> finalDeleteList = new ArrayList<TupleBeanTriple<String, String, String>>();
	private ArrayList<TupleBeanTriple<String, String, String>> finalUpdateList = new ArrayList<TupleBeanTriple<String, String, String>>();
	private Connection conn;
	private ConnectionValidator validator = new ConnectionValidator();
	private ConnectionManager connMgr;
	private String path;
	public ArrayList<Cell[]> dataSource;

	/* Class initialization with Properties */
	public FileDataHandler(String path) throws IOException {

		this.path = path;
		connMgr = new ConnectionManager();
		conn = connMgr.createNew();
		validator = new ConnectionValidator();
	}

	/*
	 * Delete current old file from local
	 */
	// Delete the current old file
	// new FileHandler().deleteOldFile(prop.getProperty("file.path") +
	// "dr_directory_old.xls");
	public void deleteOldFile(String path) {

		File file = new File(path);

		if (file.delete()) {
			logger.warn(file.getName() + " is deleted");
		} else {
			logger.error("Delete operation is failed.");
		}
	}

	/*
	 * Rename the current latest file to be old file
	 */
	// Rename the current latest file to old file
	// new FileHandler().renameFile(path, new File(prop.getProperty("file.path")
	// + "dr_directory_latest.xls"));
	public void renameFile(String path, File currentFile) {

		File newName = new File(path + "dr_directory_old.xls");
		if (currentFile.renameTo(newName)) {
			logger.info("Renamed the current latest file to current old file");
		} else {
			logger.error("Error");
		}
	}

	/*
	 * Move all files to the old folder
	 */
	public void moveFile() throws IOException {

		File dir = new File(this.path);

		List<File> list = Arrays.asList(dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				Optional<String> nameObj = Optional.ofNullable(name);
				return nameObj.orElse("").endsWith(".xls"); // or something
															// else
			}
		}));

		String listFiles = list.stream().map(l -> l.toString().replace("./", "")).collect(Collectors.toList())
				.toString();

		logger.info(listFiles);

		for (File file : list) {
			if (file.renameTo(new File(path + "old/" + file.getName()))) {
				logger.info("Renamed the " + file.getName() + " file to current old file");
			} else {
				logger.error("Error to move the file - " + file.getName());
			}
		}
	}

	/* Get the file which is the most modified */
	public String getLastModifiedFile(String type) {

		File dir = new File(this.path + "" + type);

		List<File> list = Arrays.asList(dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				Optional<String> nameObj = Optional.ofNullable(name);
				return nameObj.orElse("").endsWith(".xls"); // or something else
			}
		}));

		ArrayList<TupleBean<Long, String>> fileBeans = (ArrayList<TupleBean<Long, String>>) list.stream()
				.map(l -> new TupleBean<Long, String>(l.lastModified(), l.getName())).collect(Collectors.toList());

		logger.debug(fileBeans.toString());

		Long fileSize = fileBeans.stream().map(l -> l.getX()).reduce((a, b) -> Math.max(a, b)).get();

		List<TupleBean<Long, String>> file = fileBeans.stream()
				.filter(l -> l.getX().toString().equals(fileSize.toString())).collect(Collectors.toList());

		logger.debug("File name (Size) : " + file.get(0).getY().trim() + "(" + file.get(0).getX() + ")");

		return file.get(0).getY().trim();
	}

	/*
	 * Download file from web and save to the local path
	 */
	public void downloadFile(String fileURL, String saveDir, int BUFFER_SIZE) throws IOException {

		DateTimeFormatter formatter = GrabAdrDiff.formatter;
		URL url = new URL(fileURL);

		HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();

		/* Searching criteria for getting report */
		String paraml = "xlParam=%7B%22region%22%3A%22%22%2C%22countryCode%22%3A%22%22%2C%22industryCode%22%3A%22%22%2C%22exchange%22%3A%22%22%2C%22depositaryBank%22%3A%22%22%2C%22sponsorship%22%3A%22%22%2C%22capitalRaised%22%3A%22%22%2C%22fromDate%22%3A%22%22%2C%22toDate%22%3A%22%22%2C%22letter%22%3A%22%22%2C%22drType%22%3A%22A%22%2C%22reportType%22%3A%22%22%2C%22searchType%22%3A%221%22%2C%22searchText%22%3A%22%22%2C%22symbol%22%3A%22%22%2C%22cusip%22%3A%22%22%2C%22year%22%3A%22%22%2C%22companyName%22%3A%22%22%2C%22limit%22%3A-1%2C%22count%22%3A50%2C%22start%22%3A0%2C%22xlRptType%22%3A%22drdirectory%22%2C%22filename%22%3A%22dr_directory%22%7D";

		byte[] postData = paraml.getBytes(StandardCharsets.UTF_8);

		httpConn.setRequestProperty("Cookie", "AgrtCookie=agreed;");
		httpConn.setRequestMethod("POST");
		httpConn.setDoOutput(true);
		httpConn.setRequestProperty("Content-Length", "700");
		httpConn.getOutputStream().write(postData);

		int responseCode = httpConn.getResponseCode();

		if (responseCode == HttpURLConnection.HTTP_OK) {

			String fileName = "";
			String disposition = httpConn.getHeaderField("Content-Disposition");
			String contentType = httpConn.getContentType();
			int contentLength = httpConn.getContentLength();

			/* Setting file name */
			if (disposition != null) {
				int index = disposition.indexOf("filename=");
				if (index > 0) {
					fileName = LocalDateTime.now().format(formatter) + ".xls";
				}
			} else {
				fileName = LocalDateTime.now().format(formatter) + ".xls";
			}

			logger.debug("Content-Type = " + contentType);
			logger.debug("Content-Disposition = " + disposition);
			logger.debug("Content-Length = " + contentLength);
			logger.info("fileName = " + fileName);

			/* Get file by stream */
			InputStream inputStream = httpConn.getInputStream();
			String saveFilePath = saveDir + File.separator + fileName;

			/* Generate file by stream */
			FileOutputStream outputStream = new FileOutputStream(saveFilePath);

			/* Read the file as bytes stream and output the file to file path */
			int bytesRead = -1;
			byte[] buffer = new byte[BUFFER_SIZE];
			while ((bytesRead = inputStream.read(buffer)) != -1) {
				outputStream.write(buffer, 0, bytesRead);
			}

			outputStream.close();
			inputStream.close();

			logger.info("File downloaded");

		} else {
			logger.warn("No file to download. Server replied HTTP code: " + responseCode);
		}

		httpConn.disconnect();
	}

	/* Read all rows and save to the array list for finding difference */
	public void readFromExcel(String path) {

		dataSource = new ArrayList<Cell[]>();

		try {

			InputStream in = new FileInputStream(new File(path));
			Workbook wrkBk = Workbook.getWorkbook(in);
			Sheet sheet = wrkBk.getSheet(0);

			for (int i = 1; i < sheet.getRows(); i++) {

				Cell[] row = Arrays.copyOfRange(sheet.getRow(i), 0, 8);
				dataSource.add(row);
			}

			logger.info("Importing the file : " + path);

		} catch (BiffException | IOException e) {

			logger.error(e);
		}
	}

	/* Handle 2 special characters of some countries - Peru and Mexico */
	private String handleSpecialCountryName(String country) {

		if (country.matches("M(.+)xico")) {

			country = "Mexico";

		} else if (country.matches("Per(.+)")) {

			country = "Peru";
		}

		return country;
	}

	/* The entry method to be used to find difference */
	public void findDifference(ArrayList<Cell[]> oldDataSource) throws InterruptedException, ExecutionException {

		/*
		 * this.getDataSource() == Latest one, oldDataSource == Older one
		 */
		ExecutorService executor = Executors.newFixedThreadPool(4);

		logger.debug("Original Size of this.getDataSource: " + this.getDataSource().size());
		logger.debug("Original Size of oldDataSource: " + oldDataSource.size());

		/* Invoke all threads to get the list of differences */
		List<Future<ArrayList<CellBean>>> results = executor.invokeAll(Arrays.asList(
				new differenceFinder(oldDataSource, this.getDataSource(), 0, this.getDataSource().size() / 2, "N"),
				new differenceFinder(oldDataSource, this.getDataSource(), (this.getDataSource().size() / 2) + 1,
						this.getDataSource().size(), "N"),
				new differenceFinder(this.getDataSource(), oldDataSource, 0, oldDataSource.size() / 2, "D"),
				new differenceFinder(this.getDataSource(), oldDataSource, (oldDataSource.size() / 2) + 1,
						oldDataSource.size(), "D")));
		executor.shutdown();

		/* Streaming operation for listing out the result */
		ArrayList<TupleBeanTriple<String, String, String>> tupleList = (ArrayList<TupleBeanTriple<String, String, String>>) results
				.stream().map(list -> {
					try {
						return list.get();
					} catch (InterruptedException | ExecutionException e) {
						e.printStackTrace();
					}
					return null;
				}).flatMap(list -> list.stream())
				.map(list -> list.getRow()[1].getContents() + "#" + list.getStatus() + "#" + list.getDifference())
				.distinct().map(list -> new TupleBeanTriple<String, String, String>(list.split("#")[0],
						list.split("#")[1], list.split("#")[2]))
				.collect(Collectors.toList());

		/*
		 * logger.debug("The total result of comparing between 2 files -> " +
		 * tupleList);
		 */
		ArrayList<TupleBeanTriple<String, String, String>> addOrDeleteList = (ArrayList<TupleBeanTriple<String, String, String>>) tupleList
				.stream().filter(l -> "N".equals(l.getY()) || "D".equals(l.getY())).collect(Collectors.toList());

		ArrayList<TupleBeanTriple<String, String, String>> updateList = (ArrayList<TupleBeanTriple<String, String, String>>) tupleList
				.stream().filter(l -> "U".equals(l.getY())).collect(Collectors.toList());

		logger.debug("Needed to be Added and Deleted -> " + addOrDeleteList);

		logger.debug("Needed to be Updated -> " + updateList);

		/*
		 * Find which one is needed to be added or deleted to compare with
		 * Database
		 */
		if (tupleList.size() > 0) {

			try {

				if (validator.isValid(conn)) {

					logger.info("Start to compare for finding differences ... ");

					findDifferenceWithDatabaseWhichNeededToBeAddedOrDeleted(
							(ArrayList<TupleBeanTriple<String, String, String>>) tupleList.stream()
									.filter(l -> "N".equals(l.getY())).collect(Collectors.toList()),
							conn, "NEW");
					findDifferenceWithDatabaseWhichNeededToBeAddedOrDeleted(
							(ArrayList<TupleBeanTriple<String, String, String>>) tupleList.stream()
									.filter(l -> "D".equals(l.getY())).collect(Collectors.toList()),
							conn, "DELETE");
					findDifferenceWithDatabaseWhichNeededToBeUpdated(
							(ArrayList<TupleBeanTriple<String, String, String>>) tupleList.stream()
									.filter(l -> "U".equals(l.getY())).collect(Collectors.toList()),
							conn, "UPDATE");

					logger.info("Total : " + (finalNewList.size() + finalDeleteList.size() + finalUpdateList.size())
							+ " differences have been found between latest and old files.");
				}

			} catch (SQLException e) {

				e.printStackTrace();
				logger.error(e);

			} finally {
				logger.info("Find difference completed");
				validator.invalidate(conn);
			}
		}

	}

	/*
	 * Find which one is needed to be added or deleted
	 */
	private void findDifferenceWithDatabaseWhichNeededToBeAddedOrDeleted(
			ArrayList<TupleBeanTriple<String, String, String>> tupleList, Connection conn, String type)
			throws SQLException {

		ArrayList<String> reutersSymbol = new ArrayList<String>();
		ArrayList<TupleBeanTriple<String, String, String>> resultList = new ArrayList<TupleBeanTriple<String, String, String>>();

		Statement stmt = conn.createStatement();

		// Get all Reuters symbols
		String sql = "SELECT DISTINCT RTRIM(ReutersSymbol) FROM Market (nolock) WHERE AreaCode = \'US\' AND ReutersSymbol IS NOT NULL";
		ResultSet rs = stmt.executeQuery(sql);
		while (rs.next()) {
			reutersSymbol.add(rs.getString(1));
		}

		String subQueryParms = "";

		if (!tupleList.isEmpty()) {

			// Get parameters of sub query
			for (TupleBean<String, String> bean : tupleList) {
				subQueryParms += createPossibleReutersSymbol(reutersSymbol, bean.getX().toString());
			}

			subQueryParms = subQueryParms.substring(0, subQueryParms.length() - 1);

			// Get the result
			sql = "SELECT RTRIM(m.Code), RTRIM(s.Symbol) FROM Reuters_Code_Map m (nolock), Security s (nolock) WHERE m.Code = s.Code AND ReutersCode IN ("
					+ subQueryParms + ")";

			rs = stmt.executeQuery(sql);

			while (rs.next()) {
				resultList.add(new TupleBeanTriple<String, String, String>(rs.getString(1), rs.getString(2), null));
			}

			if ("NEW".equals(type))

				setFinalNewList(resultList);

			else if ("DELETE".equals(type)) {

				setFinalDeleteList(resultList);
			}
		}

		// Display the result
		if (resultList.size() != 0)
			logger.debug(type + " : " + resultList.size() + " Record(s) with Symbol have been found -> " + resultList);
		else
			logger.debug(type + " : Record with Symbol: " + resultList.size()
					+ " found, but not reported since not in our database : "
					+ tupleList.stream().map(l -> l.getX()).collect(Collectors.toList()));
	}

	/*
	 * Find which one is needed to be updated
	 */
	private void findDifferenceWithDatabaseWhichNeededToBeUpdated(
			ArrayList<TupleBeanTriple<String, String, String>> tupleList, Connection conn, String type)
			throws SQLException {

		ArrayList<String> reutersSymbol = new ArrayList<String>();
		ArrayList<TupleBeanTriple<String, String, String>> resultList = new ArrayList<TupleBeanTriple<String, String, String>>();
		ArrayList<TupleBean<String, String>> sqlBatchQueue = new ArrayList<TupleBean<String, String>>();

		Statement stmt = conn.createStatement();

		// Get all Reuters symbols
		String sql = "SELECT DISTINCT RTRIM(ReutersSymbol) FROM Market (nolock) WHERE AreaCode = \'US\' AND ReutersSymbol IS NOT NULL";
		ResultSet rs = stmt.executeQuery(sql);
		while (rs.next()) {
			reutersSymbol.add(rs.getString(1));
		}

		String subQueryParms = "";

		if (!tupleList.isEmpty()) {

			// Get parameters of sub query and create SQL batch
			for (TupleBeanTriple<String, String, String> bean : tupleList) {
				subQueryParms = createPossibleReutersSymbol(reutersSymbol, bean.getX().toString());
				subQueryParms = subQueryParms.substring(0, subQueryParms.length() - 1);
				sql = "SELECT RTRIM(m.Code), RTRIM(s.Symbol), '" + bean.getZ().toString()
						+ "' FROM Reuters_Code_Map m (nolock), Security s (nolock) WHERE m.Code = s.Code AND ReutersCode IN ("
						+ subQueryParms + ")";
				sqlBatchQueue.add(new TupleBean<String, String>(sql, ""));
			}

			sqlBatchQueue = (ArrayList<TupleBean<String, String>>) sqlBatchQueue.stream().distinct()
					.collect(Collectors.toList());

			Iterator<TupleBean<String, String>> iter = sqlBatchQueue.iterator();

			while (iter.hasNext()) {
				String sqlTmp = iter.next().getX();
				rs = stmt.executeQuery(sqlTmp);
				while (rs.next()) {
					resultList.add(new TupleBeanTriple<String, String, String>(rs.getString(1), rs.getString(2),
							rs.getString(3)));
					break;
				}
			}

			setFinalUpdateList(resultList);
		}

		// Display the result
		if (resultList.size() != 0)
			logger.debug(type + " : " + resultList.size() + " Record(s) with Symbol have been found (Latest, Older) -> "
					+ resultList.stream().distinct().collect(Collectors.toList()));
		else
			logger.debug(type + " : " + "Record with Symbol: " + resultList.size()
					+ " found, but not reported since not in our database : "
					+ tupleList.stream().map(l -> l.getX()).collect(Collectors.toList()));

	}

	// Zip and combine the Symboms with parms
	private String createPossibleReutersSymbol(List<String> symbols, String parms) {

		String result = "";
		LinkedList<String> parmsList = new LinkedList<String>();
		Iterator<String> iter = symbols.iterator();

		while (iter.hasNext()) {
			parmsList.add(parms + "." + iter.next());
		}

		for (String str : parmsList) {
			result += "\'" + str + "\',";
		}

		return result;
	}

	/* Connect to database and delete redundant records */
	public void deleteFromDatabase() throws SQLException {

		conn = connMgr.getInstance(conn);
		conn.setAutoCommit(true);

		try {

			// Valid the connection is valid or not firstly
			if (validator.isValid(conn)) {

				logger.info("Starting to delete from database ... ");

				Statement stmt = conn.createStatement();

				List<String> parameters = finalDeleteList.stream().map(l -> "\'" + l.getX() + "\'")
						.collect(Collectors.toList());

				logger.debug("Will be deleted later -> " + parameters);

				Iterator<String> iter = parameters.iterator();

				while (iter.hasNext()) {

					String sql = "DELETE FROM ADR_Mapping WHERE SecurityCode = " + iter.next().toString();
					logger.debug("Generated SQL -> " + sql);
				}

				// Delete execution
				/* stmt.executeUpdate(sql); */

			}

		} finally {

			validator.invalidate(conn);
		}
	}

	/* Thread Executor for finding differences between 2 files */
	protected class differenceFinder implements Callable<ArrayList<CellBean>> {

		private ArrayList<Cell[]> latestDataSource;
		private ArrayList<Cell[]> oldDataSource;
		private final int start;
		private final int end;
		private String type;

		public differenceFinder(ArrayList<Cell[]> oldDataSource, ArrayList<Cell[]> latestDataSource, int start, int end,
				String type) {

			this.oldDataSource = oldDataSource;
			this.latestDataSource = latestDataSource;
			this.start = start;
			this.end = end;
			this.type = type;
		}

		@Override
		public ArrayList<CellBean> call() throws Exception {

			ArrayList<CellBean> diffList = new ArrayList<CellBean>();
			boolean isDiff = false;
			String isUpdated = "";

			for (int i = start; i < end; i++) {

				ArrayList<TupleBean<String, String>> tmpList = new ArrayList<TupleBean<String, String>>();

				for (int j = 0; j < oldDataSource.size(); j++) {

					if (!latestDataSource.get(i)[2].getContents().trim()
							.equals(oldDataSource.get(j)[2].getContents().trim())) {

						/*
						 * Describe which column should be handled by NEW /
						 * DELETED status
						 */
						if (j == oldDataSource.size() - 1) {
							isDiff = true;
						}

						isUpdated = "";

					} else {

						/*
						 * Describe which columns [5-7] should be handled by
						 * UPDATED status
						 * 
						 * TupleBean (Latest, Older)
						 */
						if (!latestDataSource.get(i)[5].getContents().trim()
								.equals(oldDataSource.get(j)[5].getContents().trim())) {
							isDiff = true;
							isUpdated = "U";
							if (type == "N")
								tmpList.add(new TupleBean<String, String>(latestDataSource.get(i)[5].getContents(),
										oldDataSource.get(j)[5].getContents()));
							else
								tmpList.add(new TupleBean<String, String>(oldDataSource.get(j)[5].getContents(),
										latestDataSource.get(i)[5].getContents()));
							break;
						}
						if (!handleSpecialCountryName(latestDataSource.get(i)[6].getContents())
								.equals(handleSpecialCountryName(oldDataSource.get(j)[6].getContents()))) {
							isDiff = true;
							isUpdated = "U";
							if (type == "N")
								tmpList.add(new TupleBean<String, String>(
										handleSpecialCountryName(latestDataSource.get(i)[6].getContents().trim()),
										handleSpecialCountryName(oldDataSource.get(j)[6].getContents().trim())));
							else
								tmpList.add(new TupleBean<String, String>(
										handleSpecialCountryName(oldDataSource.get(j)[6].getContents().trim()),
										handleSpecialCountryName(latestDataSource.get(i)[6].getContents().trim())));
							break;
						}
						if (!latestDataSource.get(i)[7].getContents().equals(oldDataSource.get(j)[7].getContents())) {
							isDiff = true;
							isUpdated = "U";
							if (type == "N")
								tmpList.add(
										new TupleBean<String, String>(latestDataSource.get(i)[7].getContents().trim(),
												oldDataSource.get(j)[7].getContents()));
							else
								tmpList.add(new TupleBean<String, String>(oldDataSource.get(j)[7].getContents().trim(),
										latestDataSource.get(i)[7].getContents()));
							break;
						}

						isDiff = false;
						break;
					}
				}

				// Set the status of Data to the list and be returned to outside
				if (isDiff) {
					if ("U".equals(isUpdated))
						diffList.add(new CellBean(latestDataSource.get(i), isUpdated, tmpList));
					else {
						diffList.add(new CellBean(latestDataSource.get(i), this.type, tmpList));
					}
				}
			}

			logger.debug(Thread.currentThread().getName() + " -> No of Differences -> " + diffList.size());

			return diffList;
		}
	}

	/* Get dataSource */
	public ArrayList<Cell[]> getDataSource() {

		return this.dataSource;
	}

	// Set and get final result list

	public ArrayList<TupleBeanTriple<String, String, String>> getFinalNewList() {
		return finalNewList;
	}

	private void setFinalNewList(ArrayList<TupleBeanTriple<String, String, String>> finalNewList) {
		this.finalNewList = finalNewList;
	}

	public ArrayList<TupleBeanTriple<String, String, String>> getFinalDeleteList() {
		return finalDeleteList;
	}

	private void setFinalDeleteList(ArrayList<TupleBeanTriple<String, String, String>> finalDeleteList) {
		this.finalDeleteList = finalDeleteList;
	}

	public ArrayList<TupleBeanTriple<String, String, String>> getFinalUpdateList() {
		return finalUpdateList;
	}

	private void setFinalUpdateList(ArrayList<TupleBeanTriple<String, String, String>> finalUpdateList) {
		this.finalUpdateList = finalUpdateList;
	}

}
