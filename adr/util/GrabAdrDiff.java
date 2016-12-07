package com.boom.marketUpdate.adr.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import javax.mail.Address;
import javax.mail.internet.InternetAddress;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.boom.genesys.clientEngine.ClientEngine;

public class GrabAdrDiff {

	public static String path = "";
	public final static Logger logger = Logger.getLogger(GrabAdrDiff.class);
	public final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

	public static void main(String[] args) {

		/* Command line operation definition */
		Options options = new Options();
		CommandLineParser gnuParser = (CommandLineParser) new GnuParser();
		HelpFormatter helpFormatter = new HelpFormatter();
		CommandLine cmd;

		Properties prop = new Properties();
		InputStream input = null;

		// Argument usage
		String usage = "By default, this scripts compare two files and grab the differences of the ADRs. "
				+ "if the option -FROMFILE is not provided, it uses the filename YYYY-MM-DD.xls "
				+ "where the MM is the last month and DD is the same day (special case for leap years). "
				+ " Also if the option -TOFILE is not provided, "
				+ "it uses the filename YYYY-MM-DD.xls where YYYY-MM-DD is today's date.";

		// Get all the argument's parameters
		options.addOption(new Option("d", "DEBUG", false, "Open the Debug Mode and print all debug message"));
		options.addOption(new Option("dl", "DOWNLOAD", false, "Download the latest file from web"));
		Option compareOpt = new Option("c", "COMPARE", false,
				"Compare the files automatically by default latest file and old file");
		compareOpt.setRequired(true);
		options.addOption(compareOpt);
		options.addOption(new Option("sd", "SENDMAIL", false, "Send the result to provided email address."));
		options.addOption(new Option("f", "FROMFILE", true, "Compare from the file with provided filename."));
		options.addOption(new Option("t", "TOFILE", true, "Compare to the file with provided filename."));
		options.addOption(new Option("dt", "NODBDELETE", false, "Delete redundent records automatically"));
		Option configOpt = new Option("cf", "CONFIG", false, "Set the config file path");
		configOpt.setRequired(true);
		options.addOption(configOpt);

		try {
			// Command started
			cmd = gnuParser.parse(options, args);
			path = cmd.getOptionValue("cf", "C:/ADR/");

			// Config log4j
			PropertyConfigurator.configure(path + "log4j.properties");

			// Add clientEngine instance
			ClientEngine.getInstance();

			/* Load the properties file */
			input = new FileInputStream(path + "config.properties");
			prop.load(input);

			// Open debug mode
			if (cmd.hasOption("d")) {
				logger.debug("DEBUG mode on");
			}

			// Open download mode
			if (cmd.hasOption("dl")) {

				logger.debug("in DOWNLOAD option -> ");
				new FileDataHandler(path).moveFile();

				// Download file from web
				new FileDataHandler(path).downloadFile(prop.getProperty("ref.link"), path,
						Integer.parseInt(prop.getProperty("file.BUFFER_SIZE")));
			}

			// Load the file and create the old file streaming
			FileDataHandler oldFilehandler = new FileDataHandler(path);

			// Load the file and create the latest file streaming
			FileDataHandler newFilehandler = new FileDataHandler(path);

			if (cmd.hasOption("c")) {

				logger.debug("in COMPARE option -> ");

				String fromFile = cmd.getOptionValue("f", "");
				String toFile = cmd.getOptionValue("t", "");

				if (fromFile.isEmpty() || toFile.isEmpty()) {

					// Read file by default arguments
					oldFilehandler.readFromExcel(path + prop.getProperty("file.oldpath")
							+ new FileDataHandler(path).getLastModifiedFile("old"));
					newFilehandler.readFromExcel(path + new FileDataHandler(path).getLastModifiedFile(""));

					// Compare with previous version file
					newFilehandler.findDifference(oldFilehandler.getDataSource());

				} else {

					if ((fromFile.isEmpty() && !toFile.isEmpty()) || !fromFile.isEmpty() && toFile.isEmpty()) {

						logger.error("Insufficient Argument : FROMFILE and TOFILE should be a pair");
						helpFormatter.printHelp(usage, options);

					} else {

						logger.debug(fromFile);
						logger.debug(toFile);

						// Read file by manual arguments
						oldFilehandler.readFromExcel(fromFile);
						newFilehandler.readFromExcel(toFile);

						// Compare with previous version file
						newFilehandler.findDifference(oldFilehandler.getDataSource());
					}
				}
			}

			// Start to delete the redundant records
			if (cmd.hasOption("dt")) {

				logger.debug("in NODBDELETE option -> ");
				newFilehandler.deleteFromDatabase();
			}

			// Start to send the email
			if (cmd.hasOption("sd")) {

				logger.debug("in SENDMAIL option -> ");

				// Get multiple recipients
				String addresses[] = prop.getProperty("email.to").toString().split("\\+");
				Address addrList[] = new Address[addresses.length];

				for (int i = 0; i < addresses.length; i++) {
					addrList[i] = new InternetAddress(addresses[i]);
				}

				// Build the Email and send it out
				MessageBuilder message = new MessageBuilder(new JavaMailManager(prop.getProperty("email.username"),
						prop.getProperty("email.password"), prop.getProperty("email.host")).getSession())
								.setFrom(prop.getProperty("email.from")).setRecipients(addrList)
								.setSubject("ADR Comparison result of "
										+ new FileDataHandler(path).getLastModifiedFile("/old") + " and "
										+ new FileDataHandler(path).getLastModifiedFile(""))
								.setContent(newFilehandler);

				// .setAttachment(prop.getProperty("file.path") + "result.xls")
				message.send();

			}

		} catch (Exception e) {

			helpFormatter.printHelp(usage, options);
			e.printStackTrace();
		}
	}
}