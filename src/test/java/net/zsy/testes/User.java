package net.zsy.testes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

public class User implements Serializable {

	public static final String TYPE = "user";

	private static final long serialVersionUID = 1L;

	private int id;
	private String name;
	private String gender;
	private int age;
	private List<String> hobbies;
	private String mobile;
	private Date birthday;

	public static UserBuilder getUserBuilder() {
		return new UserBuilder();
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public List<String> getHobbies() {
		return hobbies;
	}

	public void setHobbies(List<String> hobbies) {
		this.hobbies = hobbies;
	}

	public String getMobile() {
		return mobile;
	}

	public void setMobile(String mobile) {
		this.mobile = mobile;
	}

	public Date getBirthday() {
		return birthday;
	}

	public void setBirthday(Date birthday) {
		this.birthday = birthday;
	}

	public static class UserBuilder {

		private String[] firstNames = { "Steve", "Bill", "Harry", "Bruce",
				"Doug", "Doug", "James", "Monkey" };
		private String[] lastNames = { "Jobs", "Gates", "Potter", "Eckel",
				"Lea", "Cutting", "Gosling", "Luffy" };

		private String[] hobbies = { "Soccer", "Basketball", "Eat", "Code",
				"Movie", "Music", "Cook", "Sing Song" };

		private String[] mobilePrefixes = { "131", "135", "137", "138", "150",
				"158", "159", "181", "189" };

		public List<User> genUsers(int count) {
			Set<Integer> ids = new HashSet<Integer>();
			List<User> result = new ArrayList<User>(count);
			for (int i = 0; i < count; i++) {
				User user = new User();
				int id = RandomUtils.nextInt(100000, 999999);
				while (ids.contains(id)) {
					id = RandomUtils.nextInt(100000, 999999);
				}
				ids.add(id);
				user.id = id;
				user.name = genName();
				user.hobbies = genHobbies();
				user.gender = genGender();
				user.age = genAge();
				user.mobile = genMobile();
				user.birthday = genBirthDay();
				result.add(user);
			}
			return result;
		}

		private int genAge() {
			Random random = new Random();
			return random.nextInt(99);
		}

		private String genMidName() {
			Random random = new Random();
			char c = (char) (random.nextInt(26) + 65 - 1);
			return String.valueOf(c);
		}

		private String genName() {
			Random random = new Random();
			return firstNames[random.nextInt(firstNames.length)] + " "
					+ genMidName() + " "
					+ lastNames[random.nextInt(lastNames.length)];
		}

		private String genGender() {
			Random random = new Random();
			if (random.nextBoolean()) {
				return "male";
			} else {
				return "female";
			}
		}

		private List<String> genHobbies() {
			Random random = new Random();
			int hobbySize = random.nextInt(3);
			List<String> hobbies = new ArrayList<String>(hobbySize);
			for (int i = 0; i < hobbySize; i++) {
				while (true) {
					int index = random.nextInt(this.hobbies.length);
					String hobby = this.hobbies[index];
					if (hobbies.contains(hobby)) {
						continue;
					} else {
						hobbies.add(hobby);
						break;
					}
				}
			}
			return hobbies;
		}

		private String genMobile() {
			Random random = new Random();
			int index = random.nextInt(mobilePrefixes.length);
			String prefix = mobilePrefixes[index];
			return prefix + RandomStringUtils.randomNumeric(8);
		}

		private Date genBirthDay() {
			Random random = new Random();
			Calendar calendar = Calendar.getInstance();
			calendar.set(1970 + random.nextInt(40), random.nextInt(12),
					random.nextInt(30), 0, 0, 0);
			return calendar.getTime();
		}
	}
}
