package de.tudarmstadt.lt.nod.model;

public class Document {

	int id;
	String content;
	String date;
	
	public Document(int aId, String aContent, String aDate){
		this.id = aId;
		this.content= aContent;
		this.date = aDate;
	}
}
