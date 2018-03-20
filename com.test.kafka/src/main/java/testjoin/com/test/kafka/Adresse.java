package testjoin.com.test.kafka;

public class Adresse {
	int id;
	String Adresse;
	public Adresse(int id, String adresse) {
		super();
		this.id = id;
		Adresse = adresse;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getAdresse() {
		return Adresse;
	}
	public void setAdresse(String adresse) {
		Adresse = adresse;
	}
}
