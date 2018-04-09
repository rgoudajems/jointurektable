package testjoin.com.test.kafka;

import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.IntStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import static java.util.stream.Collectors.toList;


public class Contact  implements HasId{
String id;
String contact;
public Contact(String id, String contact) {
	super();
	this.id = id;
	this.contact = contact;
}
public String getId() {
	return id;
}
public void setId(String id) {
	this.id = id;
}
public String getContact() {
	return contact;
}
public void setContact(String contact) {
	this.contact = contact;
}

public void parseString(String csvStr){
    StringTokenizer st = new StringTokenizer(csvStr,",");
    id = String.valueOf(st.nextToken());
    contact = st.nextToken();

}

@Override
public String toString() {
    return "Contact{" +
            "id=" + "\'"+ id +"\'"+
            ", contact:" +"\'" + contact + "\'"+
            '}';
}
public Contact() {
	super();
	// TODO Auto-generated constructor stub
}


public static final class Builder {
    private String id;
    private String contact;

    private Builder(Contact contact){
       this.id = contact.getId();
       this.contact = contact.getContact(); 
    }


    public Contact build(){
        return new Contact(id,contact);
    }

}
public static Builder builder(Contact contact){return new Builder(contact);}
public static final int NB_START_CONTACT = 2;
private List<Contact> buildContacts() {
    return IntStream.range(1, NB_START_CONTACT)
            .mapToObj(i -> new Contact())
            .collect(toList());
}

}
