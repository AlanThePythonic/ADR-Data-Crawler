package com.boom.marketUpdate.adr.util;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import javax.mail.Address;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import org.apache.log4j.Logger;

import com.boom.marketUpdate.adr.bean.TupleBeanTriple;

/* Setting up this class by Builder Pattern : From, Recipients, Subject, Content, attachment */
public class MessageBuilder {

	private Message message;
	private Address[] toAddressList;
	private String from;
	private Multipart multipart = new MimeMultipart();
	final static Logger logger = Logger.getLogger(MessageBuilder.class);

	public MessageBuilder(Session session) {

		logger.info("Configuring the email ...");
		message = new MimeMessage(session);
	}

	public MessageBuilder setFrom(String from) throws AddressException, MessagingException {

		this.from = from;
		this.message.setFrom(new InternetAddress(from));
		return this;
	}

	public MessageBuilder setRecipients(Address[] toAddressList) throws AddressException, MessagingException {

		this.toAddressList = toAddressList;
		this.message.addRecipients(Message.RecipientType.TO, toAddressList);
		logger.info("Set Recipients .. ");
		return this;
	}

	public MessageBuilder setSubject(String title) throws MessagingException {

		this.message.setSubject(title);
		return this;
	}

	public void setAttachment(String html) throws AddressException, MessagingException {

		BodyPart messageBodyPart = new MimeBodyPart();
		messageBodyPart.setText(html);
		messageBodyPart.addHeader("Content-Type", "text/html; charset=utf-8;");
		messageBodyPart.setFileName("ADR Report of " + LocalDateTime.now().format(GrabAdrDiff.formatter) + ".xls");
		logger.info("Set Attachments .. ");
		multipart.addBodyPart(messageBodyPart);
	}

	public MessageBuilder setContent(FileDataHandler handler) throws MessagingException {

		logger.info("Set Contents .. ");
		BodyPart messageBodyPart = new MimeBodyPart();

		// CSS Configuration
		String cssConfig = "<style> " + ".style0 {mso-number-format:General; " + "text-align:general; "
				+ "vertical-align:bottom;" + " white-space:nowrap; " + "mso-rotate:0; " + "mso-background-source:auto; "
				+ "mso-pattern:auto; " + "color:windowtext; " + "font-size:10.0pt; " + "font-weight:400; "
				+ "font-style:normal; " + "text-decoration:none; " + "font-family:Arial; "
				+ "mso-generic-font-family:auto; " + "mso-font-charset:0; " + "border:none; "
				+ "mso-protection:locked visible; " + "mso-style-name:Normal; mso-style-id:0;} " + ".excelText "
				+ "{mso-style-parent:style0; mso-number-format:\"\\@\";} " + ".ModifiedText {color: red;}} </style>";

		String contentDetail = "";
		String emailContentDetail = "";

		// All of result list
		ArrayList<TupleBeanTriple<String, String, String>> resultNewList = handler.getFinalNewList();
		ArrayList<TupleBeanTriple<String, String, String>> resultDeleteList = handler.getFinalDeleteList();
		ArrayList<TupleBeanTriple<String, String, String>> resultUpdateList = handler.getFinalUpdateList();

		int totalSize = resultNewList.size() + resultDeleteList.size() + resultUpdateList.size();

		// Show no of record have been found
		String messageContent = "Please open the excel report to find the details.<br/><br/><br/>";
		String noOfRecords = "";
		noOfRecords += "No. of New Record \t: " + resultNewList.size() + "<br/>";
		noOfRecords += "No. of Delete Record \t: " + resultDeleteList.size() + "<br/>";
		noOfRecords += "No. of Update Record \t: " + resultUpdateList.size() + "<br/>";
		noOfRecords += "<b>Total Changes \t:<b> " + totalSize + "<br/>";

		Iterator<TupleBeanTriple<String, String, String>> iter = resultDeleteList.iterator();

		emailContentDetail += "Needed to be deleted : <br><br>";

		/* Show the DELETE record result of email */
		logger.debug("ResultList on MessageBuilder (D)  -> " + resultDeleteList);
		while (iter.hasNext()) {
			TupleBeanTriple<String, String, String> tmp = iter.next();
			contentDetail += "<tr><td align='center' class='excelText'>" + tmp.getX() + "</td>"
					+ "<td align='center' class='excelText'>" + tmp.getY() + "</td>"
					+ "<td align='center' class='excelText'>" + "DELETE" + "</td>"
					+ "<td align='center' class='excelText'> - </td></tr>";

			emailContentDetail += tmp.getX() + ", ";
		}

		emailContentDetail = emailContentDetail.substring(0, emailContentDetail.length() - 2) + "<br><br>";
		emailContentDetail += "Needed to be added : <br><br>";

		/* Show the NEW record result of email */
		iter = resultNewList.iterator();
		logger.debug("ResultList on MessageBuilder (N)  -> " + resultNewList);
		while (iter.hasNext()) {
			TupleBeanTriple<String, String, String> tmp = iter.next();
			contentDetail += "<tr><td align='center' class='excelText'>" + tmp.getX() + "</td>"
					+ "<td align='center' class='excelText'>" + tmp.getY() + "</td>"
					+ "<td align='center' class='excelText'>" + "NEW" + "</td>"
					+ "<td align='center' class='excelText'> - </td></tr>";

			emailContentDetail += tmp.getX() + ", ";
		}

		emailContentDetail = emailContentDetail.substring(0, emailContentDetail.length() - 2) + "<br><br>";
		emailContentDetail += "Needed to be updated : <br><br>";

		/* Show the UPDATE record result of email */
		iter = resultUpdateList.iterator();
		logger.debug("ResultList on MessageBuilder (U)-> " + resultUpdateList);
		while (iter.hasNext()) {
			TupleBeanTriple<String, String, String> tmp = iter.next();
			contentDetail += "<tr><td align='center' class='excelText'>" + tmp.getX() + "</td>"
					+ "<td align='center' class='excelText'>" + tmp.getY() + "</td>"
					+ "<td align='center' class='excelText'>" + "UPDATE" + "</td><td align='center' class='excelText'>"
					+ "<font class='ModifiedText'>" + "<s>"
					+ tmp.getZ().substring(2, tmp.getZ().length() - 2).split("\\|")[1] + "</s>" + " => "
					+ tmp.getZ().substring(2, tmp.getZ().length() - 2).split("\\|")[0] + "</font></td></tr>";

			emailContentDetail += tmp.getX() + " ((Old) "
					+ tmp.getZ().substring(2, tmp.getZ().length() - 2).split("\\|")[1] + " (NEW) "
					+ tmp.getZ().substring(2, tmp.getZ().length() - 2).split("\\|")[0] + "), ";
		}

		emailContentDetail = emailContentDetail.substring(0, emailContentDetail.length() - 2) + "<br><br>";

		/* TABLE HTML CODE */
		String tableContent = "<table cellspacing='0' cellpadding='3' width='1200' border='1'>"
				+ "<tr><td align='center' align='left'> <b>SID</b> </td>" + "<td align='center'> <b>Symbol</b> </td>"
				+ "<td align='center'> <b>Action</b> </td>" + "<td align='center'> <b>Difference(s)</b> </td><tr>"
				+ contentDetail + "</table>";

		logger.debug(cssConfig + "" + tableContent);
		logger.debug(messageContent + "" + noOfRecords);

		// Add the email content message
		messageBodyPart.setContent(messageContent + "" + noOfRecords + "<br><br>" + emailContentDetail,
				"text/html; charset=utf-8");
		multipart.addBodyPart(messageBodyPart);

		// Add attachment
		this.setAttachment(cssConfig + "" + tableContent);

		return this;
	}

	public void send() throws MessagingException {

		logger.info("Sending the email to each recipient .. ");
		message.setContent(multipart);
		Transport.send(message);
		logger.info("Sent message successfully at " + LocalDateTime.now());
	}

	public Address[] getTo() {

		return this.toAddressList;
	}

	public String getFrom() {

		return this.from;
	}

}
