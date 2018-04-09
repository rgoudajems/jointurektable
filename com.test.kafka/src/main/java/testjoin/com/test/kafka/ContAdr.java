package testjoin.com.test.kafka;

import testjoin.com.test.kafka.Contact.Builder;

public class ContAdr implements HasId {
Contact contact;
Adresse adresse;
public ContAdr(Contact contact, Adresse adresse) {
	super();
	this.contact = contact;
	this.adresse = adresse;
}
public Contact getContact() {
	return contact;
}
public void setContact(Contact contact) {
	this.contact = contact;
}
public ContAdr() {
	super();
	// TODO Auto-generated constructor stub
}
public Adresse getAdresse() {
	return adresse;
}
public void setAdresse(Adresse adresse) {
	this.adresse = adresse;
}


public ContAdr withAdresse(Adresse adresse) {
	this.setAdresse(adresse);
	return this;
}

public ContAdr withContact(Contact contact) {
	this.setContact(contact);
	return this;
}

public String getId() {
	return contact.getId();
}

public static final class Builder {
    private Contact contact;
    private Adresse adresse;

    private Builder(Contact contact, Adresse adresse){
       this.contact = contact;
       this.adresse = adresse; 
    }
    private void withContact (Contact contact){
        this.contact = contact;
     }

    private void withAdresse(Adresse adresse){
        this.adresse = adresse ; 
     }

    
    private Builder(ContAdr contadr){
        this.contact = contadr.getContact();
        this.adresse = contadr.getAdresse(); 
     }  
    public ContAdr build(){
        return new ContAdr (contact,adresse);
    }

}
public static Builder builder(ContAdr contadr){return new Builder(contadr);}


}