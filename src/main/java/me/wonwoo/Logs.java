package me.wonwoo;

import java.io.Serializable;

public class Logs implements Serializable {

	private String name;
	private String email;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		Logs logs = (Logs) o;

		return (name != null ? name.equals(logs.name) : logs.name == null) && (email != null ? email.equals(logs.email) : logs.email == null);
	}

	@Override
	public int hashCode() {
		int result = name != null ? name.hashCode() : 0;
		result = 31 * result + (email != null ? email.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "Logs{" + "name='" + name + '\'' + ", email='" + email + '\'' + '}';
	}
}
