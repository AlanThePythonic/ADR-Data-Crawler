package com.boom.marketUpdate.adr.util;

import java.util.Properties;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;

public class JavaMailManager {

	private String username;
	private String password;
	private Properties props;

	public JavaMailManager(String username, String password, String host) {

		props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", host);
		props.put("mail.smtp.port", "25");

		this.username = username;
		this.password = password;
	}

	public Session getSession() {

		return Session.getInstance(props, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password);
			}
		});
	}
}
